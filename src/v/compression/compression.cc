// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/compression.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "compression/async_stream_zstd.h"
#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/internal/snappy_java_compressor.h"
#include "compression/internal/zstd_compressor.h"

#include <seastar/util/log.hh>

#include <utility>

namespace compression {

/*
 * There are no users of this, but we need to make sure it isn't compiled away
 * in release mode so that it still shows up in the registered loggers list
 * which rpk needs to be in sync with.
 *
 * Putting it here instead of in its own compilation unit seemed to have tricked
 * the optimizer. Added a log statement below too, just for good measure.
 *
 * As soon as rpk supports logger name discovery via admin api this can be fully
 * removed.
 */
ss::logger complog{"compression"};

iobuf compressor::compress(const iobuf& io, type t) {
    switch (t) {
    case type::gzip:
        return internal::gzip_compressor::compress(io);
    case type::java_snappy:
        return internal::snappy_java_compressor::compress(io);
    case type::lz4:
        return internal::lz4_frame_compressor::compress(io);
    case type::zstd:
        return internal::zstd_compressor::compress(io);
    }
    vunreachable("Unknown compression type {}", t);
}
iobuf compressor::uncompress(const iobuf& io, type t) {
    if (io.empty()) {
        throw std::runtime_error(
          fmt::format("Asked to decompress:{} an empty buffer:{}", (int)t, io));
    }
    switch (t) {
    case type::gzip:
        return internal::gzip_compressor::uncompress(io);
    case type::java_snappy:
        return internal::snappy_java_compressor::uncompress(io);
    case type::lz4:
        return internal::lz4_frame_compressor::uncompress(io);
    case type::zstd:
        return internal::zstd_compressor::uncompress(io);
    }
    vunreachable("Unknown uncompression type {}", t);
}

ss::future<iobuf> stream_compressor::compress(iobuf io, type t) {
    switch (t) {
    case type::zstd:
        return compression::async_stream_zstd_instance().compress(
          std::move(io));
    default:
        return ss::make_ready_future<iobuf>(compressor::compress(io, t));
    }
}
ss::future<iobuf> stream_compressor::uncompress(iobuf io, type t) {
    if (io.empty()) {
        return ss::make_exception_future<iobuf>(std::runtime_error(
          fmt::format(
            "Asked to decompress:{} an empty buffer:{}", (int)t, io)));
    }
    switch (t) {
    case type::zstd:
        return compression::async_stream_zstd_instance().uncompress(
          std::move(io));
    default:
        return ss::make_ready_future<iobuf>(compressor::uncompress(io, t));
    }
}

std::string_view format_as(type c) {
    switch (c) {
    case type::gzip:
        return "gzip";
    case type::java_snappy:
        return "java_snappy";
    case type::lz4:
        return "lz4";
    case type::zstd:
        return "zstd";
    }
    return "compression::type::unknown";
}

std::ostream& operator<<(std::ostream& os, const type& c) {
    return os << format_as(c);
}

} // namespace compression
