/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "rp_module.h"

#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/vint.h"
#include "vassert.h"
#include "wasm/ffi.h"

#include <algorithm>
#include <exception>
#include <ios>
#include <stdexcept>
#include <vector>
namespace wasm {

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger log("rp_wasm_module_log");
} // namespace

redpanda_module::redpanda_module(pandaproxy::schema_registry::sharded_store* s)
  : _schema_registry_store(s) {}

model::record_batch redpanda_module::for_each_record(
  const model::record_batch* input,
  ss::noncopyable_function<void(wasm_call_params)> func) {
    vassert(
      input->header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input->data());

    auto bh = batch_handle(input->header().crc);

    std::vector<record_position> record_positions;
    record_positions.reserve(input->record_count());

    while (parser.bytes_left() > 0) {
        auto start_index = parser.bytes_consumed();
        auto [size, amt] = parser.read_varlong();
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        parser.skip(size - sizeof(model::record_attributes::type) - td);
        record_positions.push_back(
          {.start_index = start_index,
           .size = size_t(size + amt),
           .timestamp_delta = int32_t(timestamp_delta)});
    }

    _call_ctx.emplace(transform_context{
      .input = input,
    });

    for (const auto& record_position : record_positions) {
        _call_ctx->current_record = record_position;
        try {
            func({
              .batch_handle = bh,
              .record_handle = record_handle(
                int32_t(record_position.start_index)),
              .record_size = int32_t(record_position.size),
              .current_record_offset = int32_t(_call_ctx->output_record_count),
            });
        } catch (...) {
            _call_ctx = std::nullopt;
            std::rethrow_exception(std::current_exception());
        }
    }

    model::record_batch::compressed_records records = std::move(
      _call_ctx->output_records);
    model::record_batch_header header = _call_ctx->input->header();
    header.size_bytes = int32_t(
      model::packed_record_batch_header_size + records.size_bytes());
    header.record_count = _call_ctx->output_record_count;
    model::record_batch batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});
    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    _call_ctx = std::nullopt;
    return batch;
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
int32_t redpanda_module::read_batch_header(
  batch_handle bh,
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
    if (!_call_ctx || bh != _call_ctx->input->header().crc) {
        return -1;
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
    return 0;
}
int32_t redpanda_module::read_record(record_handle h, ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return -1;
    }
    if (h != int32_t(_call_ctx->current_record.start_index)) {
        return -2;
    }
    if (_call_ctx->current_record.size != buf.size()) {
        // Buffer wrong size
        return -3;
    }
    iobuf_const_parser parser(_call_ctx->input->data());
    parser.skip(_call_ctx->current_record.start_index);
    parser.consume_to(buf.size(), buf.raw());
    return int32_t(buf.size());
}

bool redpanda_module::is_valid_serialized_record(
  iobuf_const_parser parser, expected_record_metadata expected) {
    try {
        auto [record_size, amt] = parser.read_varlong();
        if (size_t(record_size) != parser.bytes_left()) {
            return false;
        }
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        auto [offset_delta, od] = parser.read_varlong();
        if (expected.timestamp != timestamp_delta) {
            return false;
        }
        if (expected.offset != offset_delta) {
            return false;
        }
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
        return false;
    }
    return parser.bytes_left() == 0;
}

int32_t redpanda_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return -1;
    }
    if (_call_ctx->output_record_count >= max_output_records) {
        return -2;
    }
    iobuf b;
    b.append(buf.raw(), buf.size());
    expected_record_metadata expected{
      // The delta offset should just be the current record count
      .offset = _call_ctx->output_record_count,
      // We expect the timestamp to not change
      .timestamp = _call_ctx->current_record.timestamp_delta,
    };
    if (!is_valid_serialized_record(iobuf_const_parser(b), expected)) {
        // Invalid payload
        return -3;
    }
    _call_ctx->output_records.append_fragments(std::move(b));
    _call_ctx->output_record_count += 1;
    return int32_t(buf.size());
}

namespace {

uint32_t encoded_schema_def_size(
  const pandaproxy::schema_registry::canonical_schema_definition& def) {
    const auto& raw_schema = def.raw();
    return raw_schema().size();
}

int32_t write_encoded_schema_def(
  const pandaproxy::schema_registry::canonical_schema_definition&,
  ffi::array<uint8_t>) {
    return 0;
}

uint32_t encoded_schema_subject_size(
  const pandaproxy::schema_registry::canonical_schema_definition& def) {
    const auto& raw_schema = def.raw();
    return raw_schema().size();
}

int32_t write_encoded_schema_subject(
  const pandaproxy::schema_registry::canonical_schema_definition&,
  ffi::array<uint8_t>) {
    return 0;
}

} // namespace

ss::future<int32_t> redpanda_module::get_schema_definition_len(
  pandaproxy::schema_registry::schema_id schema_id, uint32_t* size_out) {
    if (!_schema_registry_store) {
        co_return -1;
    }
    try {
        auto schema = co_await _schema_registry_store->get_schema_definition(
          schema_id);
        *size_out = encoded_schema_def_size(schema);
    } catch (...) {
        vlog(log.warn, "error fetching schema definition {}", schema_id);
        co_return -2;
    }
    co_return 0;
}

ss::future<int32_t> redpanda_module::get_schema_definition(
  pandaproxy::schema_registry::schema_id schema_id, ffi::array<uint8_t> buf) {
    if (!_schema_registry_store) {
        co_return -1;
    }
    try {
        auto schema = co_await _schema_registry_store->get_schema_definition(
          schema_id);
        int32_t errc = write_encoded_schema_def(schema, buf);
        if (errc < 0) {
            co_return errc;
        }
        switch (schema.type()) {
        case pandaproxy::schema_registry::schema_type::avro:
            co_return 1;
        case pandaproxy::schema_registry::schema_type::protobuf:
            co_return 2;
        case pandaproxy::schema_registry::schema_type::json:
            co_return 3;
        }
        __builtin_unreachable();
    } catch (...) {
        vlog(log.warn, "error fetching schema definition {}", schema_id);
        co_return -2;
    }
}
ss::future<int32_t> redpanda_module::get_subject_schema_len(
  pandaproxy::schema_registry::subject sub,
  pandaproxy::schema_registry::schema_version version,
  uint32_t*) {
    if (!_schema_registry_store) {
        co_return -1;
    }

    try {
    } catch (...) {
        vlog(log.warn, "error fetching schema definition {}/{}", sub, version);
        co_return -2;
    }

    co_return 0;
}

ss::future<int32_t> redpanda_module::get_subject_schema(
  pandaproxy::schema_registry::subject sub,
  pandaproxy::schema_registry::schema_version version,
  ffi::array<uint8_t>) {
    if (!_schema_registry_store) {
        co_return -1;
    }
    try {
    } catch (...) {
        vlog(log.warn, "error fetching schema definition {}/{}", sub, version);
        co_return -2;
    }
    co_return 0;
}

} // namespace wasm
