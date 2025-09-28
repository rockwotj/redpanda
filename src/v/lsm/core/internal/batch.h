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
#include "lsm/core/internal/keys.h"

#include <seastar/core/future.hh>

namespace lsm::internal {

// A batch of writes that can be atomically applied.
class write_batch {
public:
    // Add a key-value pair to the database.
    //
    // REQUIRES: key.value_type is value
    void put(internal::key key, iobuf value) {
        dassert(
          key.type() == internal::value_type::value,
          "when adding a put to a batch, keys must be of value type",
          key.decode());
        _memory_usage += key.memory_usage() + value.memory_usage();
        _batch.emplace(std::move(key), std::move(value));
    }

    // Get the value for a given key.
    //
    // REQUIRES: key.value_type is tombstone
    void remove(internal::key key) {
        dassert(
          key.type() == internal::value_type::tombstone,
          "when adding a remove to a batch, keys must be of tombstone type",
          key.decode());
        iobuf value;
        _memory_usage += key.memory_usage() + value.memory_usage();
        _batch.emplace(std::move(key), std::move(value));
    }

    // The entries in the write batch.
    absl::btree_map<internal::key, iobuf>& entries() { return _batch; }
    size_t memory_usage() { return _memory_usage; }

private:
    absl::btree_map<internal::key, iobuf> _batch;
    size_t _memory_usage = 0;
};

} // namespace lsm::internal
