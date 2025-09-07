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

#include "bytes/ioarray.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"

#include <seastar/core/align.hh>

#include <sys/uio.h>

#include <iterator>

ioarray::ioarray(size_t size)
  : ioarray(uninitialized_t{}, size) {
    for (size_t offset = 0, i = 0; offset < size;
         offset += max_chunk_size, ++i) {
        _buffers[i] = ss::temporary_buffer<char>(
          std::min(max_chunk_size, size - offset));
    }
}

ioarray::ioarray(ioarray::uninitialized_t, size_t size)
  : _offset(0)
  , _size(size)
  , _buffers((size + max_chunk_size - 1) / max_chunk_size) {}

ioarray& ioarray::operator=(ioarray&& o) noexcept {
    if (this != &o) {
        this->~ioarray();
        new (this) ioarray(std::move(o));
    }
    return *this;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray ioarray::aligned(size_t alignment, size_t size) {
    if (size == 0) {
        return ioarray{0};
    }
    dassert(
      (alignment & (alignment - 1)) == 0,
      "alignment {} must be a power of two",
      alignment);
    dassert(
      size % alignment == 0,
      "size {} must be a multiple of alignment {}",
      size,
      alignment);
    ioarray c{size};
    for (size_t offset = 0, i = 0; offset < size;
         offset += max_chunk_size, ++i) {
        size_t len = std::min(max_chunk_size, size - offset);
        auto tmpbuf = ss::temporary_buffer<char>::aligned(
          alignment, ss::align_up(len, alignment));
        tmpbuf.trim(len); // trim to actual size
        c._buffers[i] = std::move(tmpbuf);
    }
    return c;
}

ioarray::string_view::operator ss::sstring() const {
    ss::sstring out(ss::sstring::initialized_later{}, size());
    char* it = out.begin();
    for (const auto& view : _views) {
        it = std::ranges::copy(view, it).out;
    }
    return out;
}

bool ioarray::string_view::operator==(std::string_view other) const {
    if (!other.starts_with(_views[0])) {
        return false;
    }
    other.remove_prefix(_views[0].size());
    return other == _views[1];
}

std::strong_ordering
ioarray::string_view::operator<=>(std::string_view other) const {
    auto cmp = _views[0] <=> other.substr(0, _views[0].size());
    if (cmp != std::strong_ordering::equal) {
        return cmp;
    }
    other.remove_prefix(_views[0].size());
    return _views[1] <=> other;
}

ioarray ioarray::copy_from(const iobuf& buf) {
    ioarray c(buf.size_bytes());
    iobuf_const_parser parser(buf);
    size_t i = 0;
    for (size_t offset = 0; offset < buf.size_bytes();
         offset += max_chunk_size) {
        size_t len = std::min(max_chunk_size, buf.size_bytes() - offset);
        auto tmpbuf = ss::temporary_buffer<char>(len);
        parser.consume_to(len, tmpbuf.get_write());
        c._buffers[i++] = std::move(tmpbuf);
    }
    return c;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray ioarray::share(size_t pos, size_t length) {
    if (length == 0) {
        return {};
    }
    dassert(
      (pos + length) <= _size,
      "pos {} + length {} must be <= size {}",
      pos,
      length,
      _size);
    pos += _offset;
    size_t start = pos / max_chunk_size;
    size_t end = (pos + length + max_chunk_size - 1) / max_chunk_size;
    ioarray out(uninitialized_t{}, (end - start) * max_chunk_size);
    size_t i = 0;
    out._offset = pos - (start * max_chunk_size);
    out._size = length;
    size_t remaining_length = out._offset + out._size;
    for (auto& buf : std::span(_buffers).subspan(start, end - start)) {
        size_t buf_size = std::min(max_chunk_size, remaining_length);
        remaining_length -= buf_size;
        out._buffers[i++] = buf.share(0, buf_size);
    }
    return out;
}

iobuf ioarray::as_iobuf() {
    iobuf out;
    for (auto& buf : _buffers) {
        size_t offset = out.empty() ? _offset : 0;
        out.append(
          std::make_unique<iobuf::fragment>(
            buf.share(offset, buf.size() - offset)));
    }
    dassert(
      out.size_bytes() == _size,
      "as_iobuf size mismatch: expected {}, got {}",
      _size,
      out.size_bytes());
    return out;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray::string_view ioarray::read_string(size_t pos, size_t length) const {
    dassert(
      length <= max_chunk_size, "length {} must be <= max_chunk_size", length);
    dassert(
      (pos + length) <= _size,
      "pos {} + length {} must be <= size {}",
      pos,
      length,
      _size);
    pos += _offset;
    auto& buf = _buffers[pos / max_chunk_size];
    const char* it = buf.begin();
    std::advance(it, pos % max_chunk_size);
    if (std::cmp_greater_equal(std::distance(it, buf.end()), length)) {
        return string_view{{it, length}};
    }
    auto& next = _buffers[(pos / max_chunk_size) + 1];
    return string_view{
      {it, buf.end()}, {next.begin(), length - std::distance(it, buf.end())}};
}

std::vector<::iovec> ioarray::as_iovec() {
    std::vector<::iovec> iov;
    iov.reserve(_buffers.size());
    for (auto& buf : _buffers) {
        char* start = buf.get_write();
        size_t offset = iov.empty() ? _offset : 0;
        std::advance(start, offset);
        iov.emplace_back(start, buf.size() - offset);
    }
    return iov;
}

char& ioarray::operator[](size_t i) {
    dassert(i < _size, "i {} must be < size {}", i, _size);
    char* it = _buffers[i / max_chunk_size].get_write();
    std::advance(it, i % max_chunk_size);
    return *it;
}

char ioarray::operator[](size_t i) const {
    dassert(i < _size, "i {} must be < size {}", i, _size);
    return _buffers[i / max_chunk_size][i % max_chunk_size];
}

void ioarray::trim_back(size_t n) {
    dassert(n <= _size, "n {} must be <= size {}", n, _size);
    _size -= n;
    size_t i = _buffers.size() - 1;
    while (n > 0) {
        auto& last_buf = _buffers[i];
        size_t amt = std::min(n, last_buf.size());
        n -= amt;
        last_buf.trim(last_buf.size() - amt);
        if (last_buf.empty()) {
            --i;
        }
    }
    auto end = _buffers.begin();
    std::advance(end, i + 1);
    if (end == _buffers.end()) {
        return;
    }
    // Absl very annoyingly does not support move operators, so we have to do it
    // the hard way by manually destructing and reconstructing in-place.
    decltype(_buffers) replacement(
      std::make_move_iterator(_buffers.begin()), std::make_move_iterator(end));
    auto* buffers_ptr = &_buffers;
    buffers_ptr->~FixedArray();
    new (buffers_ptr) absl::FixedArray(std::move(replacement));
}
