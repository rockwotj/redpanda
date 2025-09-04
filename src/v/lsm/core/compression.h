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

#include "bytes/iobuf.h"

#include <seastar/core/future.hh>

#include <cstdint>

namespace lsm {

// The type of compression
enum class compression_type : uint8_t {
    none = 0,
    zstd = 1,
    java_snappy = 2,
    lz4 = 3,
    gzip = 4,
};

// Compress the iobuf and return the compressed iobuf.
//
// REQUIRES: compression_type is not `none`.
ss::future<iobuf> compress(iobuf, compression_type);

// Uncompress the iobuf and return the uncompressed iobuf.
//
// REQUIRES: compression_type is not `none`.
ss::future<iobuf> uncompress(iobuf, compression_type);

} // namespace lsm
