/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/block/contents.h"

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>

#include <cstdlib>

namespace lsm::block {

ss::future<ss::lw_shared_ptr<contents>> contents::read(ss::file f, handle h) {
    size_t alignment = f.disk_read_dma_alignment();
    size_t adjusted_offset = ss::align_down(h.offset, alignment);
    size_t offset_delta = h.offset - adjusted_offset;
    auto array = ioarray::aligned(
      alignment, ss::align_up(h.size + offset_delta, alignment));
    size_t amt = co_await f.dma_read(adjusted_offset, array.as_iovec());
    if (amt != array.size()) {
        throw std::runtime_error(
          fmt::format(
            "short read: failed to read {} bytes from block at offset {}, got: "
            "{}",
            array.size(),
            adjusted_offset,
            amt));
    }
    co_return ss::make_lw_shared<contents>(
      contents(array.share(offset_delta, h.size)));
}

ss::lw_shared_ptr<contents> contents::copy_from(const iobuf& buf) {
    return ss::make_lw_shared<contents>(contents(ioarray::copy_from(buf)));
}

// NOLINTNEXTLINE(*swappable-parameters*)
iobuf contents::share(size_t pos, size_t length) {
    return _data.share(pos, length).as_iobuf();
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray::string_view contents::read_string(size_t pos, size_t length) const {
    return _data.read_string(pos, length);
}

contents::contents(ioarray data)
  : _data(std::move(data)) {}

uint32_t contents::read_fixed32(size_t offset) const {
    auto last_word_slice = ss::sstring(read_string(offset, sizeof(uint32_t)));
    uint32_t v = 0;
    std::memcpy(&v, last_word_slice.data(), last_word_slice.size());
    v = ss::le_to_cpu(v);
    return v;
}
} // namespace lsm::block
