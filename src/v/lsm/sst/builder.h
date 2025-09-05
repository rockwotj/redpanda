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
#include "base/units.h"
#include "lsm/block/builder.h"
#include "lsm/block/filter.h"
#include "lsm/block/handle.h"
#include "lsm/core/compression.h"
#include "lsm/core/keys.h"

#include <seastar/core/iostream.hh>

namespace lsm::sst {

// builder provides the interface used to build a Sorted String Table (SST),
// which is an immutable and sorted map from keys to values.
class builder {
public:
    // Options for the builder.
    struct options {
        // The target block size for the SST, note that this is measured by
        // uncompressed size, so for compressed tables actual blocks might be
        // much smaller.
        size_t block_size = 4_KiB;
        // The compression type to use for SST blocks.
        compression_type compression;
    };

    // Construct a new builder that will write to the given file.
    static ss::future<builder> create(ss::file, options);

    // Add key, value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    ss::future<> add(core::internal_key key, iobuf value);

    // Finish building the table. Stops using the file passed to the
    // constructor after this function returns.
    ss::future<> finish();

    // Close the builder. This menthod must be called after any exceptions or
    // after finish is called in the case of a successful table build.
    ss::future<> close();

    // The number of calls to `add` so far.
    size_t num_entries() const;
    // Size of the file generated so far.
    //
    // If invoked after `finish`, returns the size of the final generated file.
    size_t file_size() const;

private:
    explicit builder(ss::output_stream<char>&&, options);

    ss::future<block::handle> write_raw_block(iobuf, compression_type);
    ss::future<> flush();

    size_t _added_entries = 0;
    size_t _written_bytes = 0;
    bool _pending_index_entry = false;
    block::builder _data_block;
    block::builder _index_block;
    block::handle _pending_handle;
    core::internal_key _last_key;
    std::optional<block::filter_builder> _filter;
    ss::output_stream<char> _output;
    options _opts;
};

} // namespace lsm::sst
