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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "lsm/block/contents.h"
#include "lsm/core/keys.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <cstddef>

namespace lsm::block {

// A filter_builder is used to construct filters for a SST.
// It generates a single value of data that is stored as a special block in the
// table.
class filter_builder {
public:
    void start_block(size_t block_offset);
    void add_key(core::internal_key_view key);
    iobuf finish();

private:
    void generate_filter();

    chunked_vector<ss::sstring> _keys;
    chunked_vector<uint32_t> _filter_offsets;
    iobuf _filter;
};

// A reader for a filter block in an SST.
class filter_reader {
public:
    explicit filter_reader(ss::lw_shared_ptr<block::contents>);

    // Check if it's possible that the user's key exists in the block at this
    // offset within the SST.
    bool key_may_match(uint64_t block_offset, core::internal_key_view key);

private:
    ss::lw_shared_ptr<block::contents> _contents;
    size_t _offset; // The offset at which the data ends
    size_t _num;
    uint8_t _base_lg;
};

} // namespace lsm::block
