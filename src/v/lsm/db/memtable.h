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
#include "lsm/core/internal/keys.h"

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
    // REQUIRES: key.value_type is value
    void add(internal::key key, iobuf value);

    // Remove a key-value pair to the memtable.
    //
    // REQUIRES: key.value_type is tombstone
    void remove(internal::key key);

    // Get the value for a given key.
    //
    // The offset here limits values to those that were written at or before
    // the given offset.
    //
    // REQUIRES: key.value_type is value
    std::optional<iobuf> get(internal::key_view);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm
