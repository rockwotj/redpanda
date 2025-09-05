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

#pragma once

#include "bytes/iobuf.h"
#include "lsm/block/handle.h"

namespace lsm::sst {

// Footer encapsulates the fixed information stored at the tail end of every
// table file.
struct footer {
    block::handle metaindex_handle;
    block::handle index_handle;

    // Encoded length of the footer. It consists of two block handles and a
    // magic number.
    constexpr static size_t encoded_length = 2 * sizeof(block::handle)
                                             + sizeof(uint64_t);

    bool operator==(const footer&) const = default;
    // Encode this footer as an iobuf.
    iobuf as_iobuf() const;
    // Decode this footer as an iobuf.
    static footer from_iobuf(iobuf);
};

} // namespace lsm::sst
