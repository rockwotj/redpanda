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

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/sstring.hh>

namespace lsm::db {

// A memtable is a sorted map that stores key-value pairs in memory.
//
// Additionally, it stores the version of each key, which allows a third
// dimension to the key-value pairs, allowing snapshot isolation for reads
// by limiting to a version of the memtable.
//
// It is used to buffer writes before they are flushed to disk.
class memtable : public ss::enable_lw_shared_from_this<memtable> {
public:
    using table = absl::btree_map<internal::key, iobuf, std::less<>>;

    memtable() noexcept = default;
    memtable(const memtable&) = delete;
    memtable& operator=(const memtable&) = delete;
    memtable(memtable&&) noexcept = delete;
    memtable& operator=(memtable&&) noexcept = delete;
    ~memtable() = default;

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

    // Create an iterator for this memtable.
    //
    // This iterator is safe to use in face of concurrent updates
    std::unique_ptr<internal::iterator> create_iterator();

private:
    table _table;
};

} // namespace lsm::db
