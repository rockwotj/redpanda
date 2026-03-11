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

#include "model/record.h"

namespace model {

bool record_batch_copy_iterator::has_next() const noexcept {
    return _index < _record_count;
}

model::record record_batch_copy_iterator::next() {
    auto r = model::parse_one_record_copy_from_buffer(_parser);
    ++_index;
    // if we're done, then check that we read all the buffer
    if (!has_next() && _parser.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Record iteration stopped with {} bytes remaining",
            _parser.bytes_left()));
    }
    return r;
}

record_batch_copy_iterator
record_batch_copy_iterator::create(const model::record_batch& b) {
    b.verify_iterable();
    return {b.record_count(), iobuf_const_parser(b._records)};
}

record_batch_copy_iterator::record_batch_copy_iterator(
  int32_t rc, iobuf_const_parser p)
  : _record_count(rc)
  , _parser(std::move(p)) {}

bool record_batch_iterator::has_next() const noexcept {
    return _index < _record_count;
}

model::record record_batch_iterator::next() {
    auto r = model::parse_one_record_from_buffer(_parser);
    ++_index;
    // if we're done, then check that we read all the buffer
    if (!has_next() && _parser.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Record iteration stopped with {} bytes remaining",
            _parser.bytes_left()));
    }
    return r;
}

record_batch_iterator record_batch_iterator::create(model::record_batch&& b) {
    b.verify_iterable();
    return {b.record_count(), iobuf_parser(std::move(b).release_data())};
}

record_batch_iterator::record_batch_iterator(int32_t rc, iobuf_parser p)
  : _record_count(rc)
  , _parser(std::move(p)) {}

fmt::iterator tx_range::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "pid: {}, range: [{}, {}]", pid, first, last);
}

void record_batch_header::reset_size_checksum_metadata(const iobuf& records) {
    size_bytes = model::packed_record_batch_header_size + records.size_bytes();
    crc = model::crc_record_batch(*this, records);
    header_crc = model::internal_header_only_crc(*this);
}

} // namespace model
