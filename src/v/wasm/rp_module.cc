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
#include "utils/vint.h"
#include "vassert.h"

#include <exception>
#include <ios>
#include <vector>
namespace wasm {

model::record_batch redpanda_module::for_each_record(
  const model::record_batch* input,
  ss::noncopyable_function<void(wasm_call_params)> func) {
    vassert(
      input->header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input->data());

    batch_handle bh = input->header().crc;

    std::vector<record_position> record_positions;
    record_positions.reserve(input->record_count());

    while (parser.bytes_left() > 0) {
        auto offset = parser.bytes_consumed();
        auto [size, amt] = parser.read_varlong();
        parser.skip(size);
        record_positions.emplace_back(offset, size + amt);
    }

    _call_ctx.emplace(transform_context{
      .input = input,
    });

    for (const auto& record_position : record_positions) {
        _call_ctx->current_record = record_position;
        try {
            func({
              .batch_handle = bh,
              .record_handle = int32_t(record_position.offset),
              .record_size = int32_t(record_position.size),
            });
        } catch (const std::exception& ex) {
            _call_ctx = std::nullopt;
            throw ex;
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
  batch_handle,
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
    if (!_call_ctx) {
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
    if (_call_ctx || h < 0 || h >= _call_ctx->input->record_count()) {
        return -1;
    }
    // TODO: Don't make this n^2, the handle should be opaque and the host
    // should advance the current position in the input data.
    iobuf_const_parser parser(_call_ctx->input->data());
    for (int i = 0; i < h; ++i) {
        auto [size, _] = parser.read_varlong();
        parser.skip(size);
    }
    auto [size, amt] = parser.read_varlong();
    if ((size + amt) < buf.size()) {
        // Buffer too small
        return -2;
    }
    size_t offset = vint::serialize(size, buf.raw());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    parser.consume_to(size, buf.raw() + offset);
    return int32_t(buf.size());
}

bool redpanda_module::is_valid_serialized_record(iobuf_const_parser parser) {
    auto [record_size, amt] = parser.read_varlong();
    if (size_t(record_size) != parser.bytes_left()) {
        return false;
    }
    parser.skip(sizeof(model::record_attributes::type));
    auto [timestamp_delta, td] = parser.read_varlong();
    auto [offset_delta, od] = parser.read_varlong();
    // TODO additional validation of the timestamp and offset
    // this is easier when we explicitly know what record we're at.
    (void)timestamp_delta;
    (void)offset_delta;
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
    return true;
}

int32_t redpanda_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return -1;
    }
    iobuf b;
    b.append(buf.raw(), buf.size());
    if (!is_valid_serialized_record(iobuf_const_parser(b))) {
        // Invalid payload
        return -2;
    }
    _call_ctx->output_records.append_fragments(std::move(b));
    _call_ctx->output_record_count += 1;
    return int32_t(buf.size());
}

} // namespace wasm
