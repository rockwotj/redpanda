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

#include "lsm/core/compression.h"

#include "compression/compression.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace lsm {

namespace {

compression::type convert_type(compression_type type) noexcept {
    switch (type) {
    case compression_type::zstd:
        return compression::type::zstd;
    case compression_type::java_snappy:
        return compression::type::java_snappy;
    case compression_type::lz4:
        return compression::type::lz4;
    case compression_type::gzip:
        return compression::type::gzip;
    case compression_type::none:
        break;
    }
    vassert(false, "unknown compression type: {}", std::to_underlying(type));
}

} // namespace

ss::future<iobuf> compress(iobuf buf, compression_type type) {
    return compression::stream_compressor::compress(
      std::move(buf), convert_type(type));
}

ss::future<ioarray> uncompress(ioarray array, compression_type type) {
    // TODO(lsm): support uncompression directly into ioarray?
    auto iobuf = co_await compression::stream_compressor::uncompress(
      array.as_iobuf(), convert_type(type));
    co_return ioarray::copy_from(iobuf);
}

compression_type compression_type_from_raw(uint8_t v) {
    auto ct = static_cast<compression_type>(v);
    switch (ct) {
    case compression_type::none:
    case compression_type::zstd:
    case compression_type::java_snappy:
    case compression_type::lz4:
    case compression_type::gzip:
        return ct;
    }
    throw std::runtime_error(fmt::format("unknown compression type: {}", v));
}

} // namespace lsm
