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
#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

namespace lsm {

// A memtable is a sorted map that stores key-value pairs in memory.
//
// Additionally, it stores the version of each key, which allows a third
// dimension to the key-value pairs, allowing snapshot isolation for reads
// by limiting to a version of the memtable.
//
// It is used to buffer writes before they are flushed to disk.
class memtable {
    class impl;

public:
    memtable() noexcept;
    memtable(const memtable&) = delete;
    memtable& operator=(const memtable&) = delete;
    memtable(memtable&&) noexcept;
    memtable& operator=(memtable&&) noexcept;
    ~memtable();

    // Add a key-value pair to the memtable.
    //
    // The offset is used as the seqno for the entry and should be
    // record's offset in the raft write-ahead log.
    void add(model::offset, ss::sstring key, iobuf value);

    // Get the value for a given key.
    //
    // The offset here limits values to those that were written at or before
    // the given offset.
    std::optional<iobuf> get(model::offset, std::string_view key);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm
