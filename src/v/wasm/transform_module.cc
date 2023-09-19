/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform_module.h"

#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "utils/named_type.h"
#include "utils/vint.h"
#include "vassert.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/thread.hh>

#include <algorithm>
#include <exception>
#include <optional>
#include <stdexcept>
#include <vector>
namespace wasm {

constexpr int32_t NO_ACTIVE_TRANSFORM = -1;
constexpr int32_t INVALID_BUFFER = -2;

ss::future<ss::chunked_fifo<model::record_batch>>
transform_module::transform(transform_args args) {
    const model::record_batch* input = args.batch;
    vassert(
      input->header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input->data());

    ss::chunked_fifo<record_position> record_positions;
    record_positions.reserve(input->record_count());
    size_t max_record_size = 0;

    while (parser.bytes_left() > 0) {
        auto start_index = parser.bytes_consumed();
        auto [size, amt] = parser.read_varlong();
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        parser.skip(size - sizeof(model::record_attributes::type) - td);
        size_t record_size = size + amt;
        max_record_size = std::max(record_size, max_record_size);
        record_positions.push_back(
          {.start_index = start_index,
           .size = record_size,
           .timestamp_delta = int32_t(timestamp_delta)});
    }

    _call_ctx.emplace(transform_context{
      .input = input,
      .max_input_record_size = max_record_size,
      .record_positions = std::move(record_positions),
      .pre_record_callback = std::move(args.pre_record_callback),
    });

    co_await signal_batch_ready();

    ss::chunked_fifo<model::record_batch> batches;
    for (output_batch& b : _call_ctx->output_batches) {
        model::record_batch::compressed_records records = std::move(b.records);
        model::record_batch_header header = _call_ctx->input->header();
        header.size_bytes = int32_t(
          model::packed_record_batch_header_size + records.size_bytes());
        header.record_count = b.record_count;
        model::record_batch batch(
          header, std::move(records), model::record_batch::tag_ctor_ng{});
        batch.header().crc = model::crc_record_batch(batch);
        batch.header().header_crc = model::internal_header_only_crc(
          batch.header());
        batches.push_back(std::move(batch));
    }
    _call_ctx = std::nullopt;
    co_return std::move(batches);
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ss::future<int32_t> transform_module::read_batch_header(
  int64_t* base_offset,
  int32_t* record_count,
  int32_t* partition_leader_epoch,
  int16_t* attributes,
  int32_t* last_offset_delta,
  int64_t* base_timestamp,
  int64_t* max_timestamp,
  int64_t* producer_id,
  int16_t* producer_epoch,
  int32_t* base_sequence) {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    co_await signal_batch_complete();
    if (!_call_ctx) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    *base_offset = _call_ctx->input->base_offset();
    *record_count = _call_ctx->input->record_count();
    *partition_leader_epoch = int32_t(_call_ctx->input->term()());
    *attributes = _call_ctx->input->header().attrs.value();
    *last_offset_delta = _call_ctx->input->header().last_offset_delta;
    *base_timestamp = _call_ctx->input->header().first_timestamp();
    *max_timestamp = _call_ctx->input->header().max_timestamp();
    *producer_id = _call_ctx->input->header().producer_id;
    *producer_epoch = _call_ctx->input->header().producer_epoch;
    *base_sequence = _call_ctx->input->header().base_sequence;

    _wasi_module->set_timestamp(_call_ctx->input->header().first_timestamp);

    co_return int32_t(_call_ctx->max_input_record_size);
}

int32_t transform_module::read_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx || _call_ctx->record_positions.empty()) {
        return NO_ACTIVE_TRANSFORM;
    }
    auto current = _call_ctx->record_positions.front();
    if (current.size < buf.size()) {
        // Buffer wrong size
        return INVALID_BUFFER;
    }
    _call_ctx->pre_record_callback();
    _call_ctx->record_positions.pop_front();
    _wasi_module->set_timestamp(model::timestamp(
      _call_ctx->input->header().first_timestamp() + current.timestamp_delta));
    _call_ctx->latest_record_timestamp_delta = current.timestamp_delta;
    iobuf_const_parser parser(_call_ctx->input->data());
    parser.skip(current.start_index);
    parser.consume_to(buf.size(), buf.data());
    return int32_t(buf.size());
}

transform_module::validation_result
transform_module::validate_serialized_record(
  iobuf_const_parser parser, expected_record_metadata expected) {
    bool start_new_batch = false;
    try {
        auto [record_size, amt] = parser.read_varlong();
        if (size_t(record_size) != parser.bytes_left()) {
            return {.is_valid = false};
        }
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        auto [offset_delta, od] = parser.read_varlong();
        if (expected.timestamp != timestamp_delta) {
            return {.is_valid = false};
        }
        // It's always valid to start a new batch
        if (expected.offset != offset_delta && offset_delta != 0) {
            return {.is_valid = false};
        }
        start_new_batch = offset_delta == 0;
        auto [key_length, kl] = parser.read_varlong();
        if (key_length > 0) {
            parser.skip(key_length);
        }
        auto [value_length, vl] = parser.read_varlong();
        if (value_length > 0) {
            parser.skip(value_length);
        }
        auto [header_count, hv] = parser.read_varlong();
        for (int i = 0; i < header_count; ++i) {
            auto [key_length, kl] = parser.read_varlong();
            if (key_length > 0) {
                parser.skip(key_length);
            }
            auto [value_length, vl] = parser.read_varlong();
            if (value_length > 0) {
                parser.skip(value_length);
            }
        }
    } catch (const std::out_of_range& ex) {
        return {.is_valid = false};
    }
    return {.is_valid = parser.bytes_left() == 0, .new_batch = start_new_batch};
}

int32_t transform_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    ss::chunked_fifo<output_batch>& output_batches = _call_ctx->output_batches;
    iobuf b;
    b.append(buf.data(), buf.size());
    expected_record_metadata expected{
      // The delta offset should just be the current record count
      .offset = output_batches.empty() ? 0 : output_batches.back().record_count,
      // We expect the timestamp to not change
      .timestamp = _call_ctx->latest_record_timestamp_delta,
    };
    auto result = validate_serialized_record(iobuf_const_parser(b), expected);
    if (!result.is_valid) {
        // Invalid payload
        return INVALID_BUFFER;
    } else if (result.new_batch) {
        output_batches.emplace_back();
    }
    output_batches.back().records.append_fragments(std::move(b));
    output_batches.back().record_count += 1;
    return int32_t(buf.size());
}

void transform_module::check_abi_version_1() {}

transform_module::transform_module(
  ffi::async_pending_host_call* pending_call,
  wasi::preview1_module* wasi_module)
  : _wasi_module(wasi_module)
  , _pending_call(pending_call) {}

void transform_module::start() {
    vlog(wasm_log.debug, "reset transform_module");
    _abort_ex = nullptr;
}

void transform_module::abort(const std::exception_ptr& ex) {
    vlog(
      wasm_log.debug,
      "aborting transform mod {} {}",
      uintptr_t(this),
      ss::thread::running_in_thread());
    _abort_ex = ex;
    if (_input_batch_ready.has_waiters()) {
        vlog(
          wasm_log.debug,
          "signalling for input_batch_ready on abort: {} {}",
          uintptr_t(this),
          ss::thread::running_in_thread());
        _input_batch_ready.signal();
    }
    if (_waiting_for_next_input_batch.has_waiters()) {
        vlog(
          wasm_log.debug,
          "signalling for _waiting_for_next_input_batch on abort: {} {}",
          uintptr_t(this),
          ss::thread::running_in_thread());
        _waiting_for_next_input_batch.signal();
    }
}
void transform_module::stop() {
    vlog(
      wasm_log.debug,
      "stopping transform mod {} {}",
      uintptr_t(this),
      ss::thread::running_in_thread());
    // this will fail
    // vassert(!_input_batch_ready.has_waiters(), "has waiters!");
    _abort_ex = std::make_exception_ptr(ss::abort_requested_exception());
    if (_waiting_for_next_input_batch.has_waiters()) {
        vlog(
          wasm_log.debug,
          "signalling for _waiting_for_next_input_batch on stop: {} {}",
          uintptr_t(this),
          ss::thread::running_in_thread());
        _waiting_for_next_input_batch.signal();
    }
}
ss::future<> transform_module::wait_ready() {
    if (_abort_ex) {
        vlog(wasm_log.debug, "wait ready throwing {}", uintptr_t(this));
        std::rethrow_exception(_abort_ex);
    }
    vlog(wasm_log.debug, "wait ready {}", uintptr_t(this));
    vassert(!_waiting_for_next_input_batch.has_waiters(), "has waiters!");
    co_await _waiting_for_next_input_batch.wait();
    if (_abort_ex) {
        vlog(wasm_log.debug, "wait ready throwing {}", uintptr_t(this));
        std::rethrow_exception(_abort_ex);
    }
}

ss::future<> transform_module::signal_batch_ready() {
    if (_abort_ex) {
        vlog(wasm_log.debug, "signal batch ready throwing {}", uintptr_t(this));
        std::rethrow_exception(_abort_ex);
    }
    vlog(wasm_log.debug, "signal batch ready {}", uintptr_t(this));
    // Allow the transform to process until it's ready for another batch
    _input_batch_ready.signal();
    vassert(!_waiting_for_next_input_batch.has_waiters(), "has waiters!");
    co_await _waiting_for_next_input_batch.wait();
    if (_abort_ex) {
        vlog(wasm_log.debug, "signal batch ready throwing {}", uintptr_t(this));
        std::rethrow_exception(_abort_ex);
    }
}
ss::future<> transform_module::signal_batch_complete() {
    if (_abort_ex) {
        vlog(
          wasm_log.debug,
          "signal_batch_complete throwing {} {}",
          ss::thread::running_in_thread(),
          uintptr_t(this));
        std::rethrow_exception(_abort_ex);
    }
    vlog(wasm_log.debug, "signal batch complete {}", uintptr_t(this));
    _waiting_for_next_input_batch.signal();
    vassert(!_input_batch_ready.has_waiters(), "has waiters!");
    co_await _input_batch_ready.wait();
    if (_abort_ex) {
        vlog(
          wasm_log.debug,
          "signal_batch_complete throwing {} {}",
          uintptr_t(this),
          ss::thread::running_in_thread());
        std::rethrow_exception(_abort_ex);
    }
}

transform_module::~transform_module() {
    vlog(wasm_log.debug, "~transform_module {}", uintptr_t(this));
};
void transform_module::set_pending_async_call(ss::future<> fut) {
    _pending_call->pending_call = std::move(fut);
}
} // namespace wasm
