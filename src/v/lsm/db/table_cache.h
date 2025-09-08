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

#include "absl/functional/function_ref.h"
#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/io/persistence.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::db {

// A table cache keeps a cache of open file handles to SST files, and supports
// accessing reads and iterators to these files.
class table_cache {
public:
    class impl;

    table_cache(io::persistence*, int32_t max_entries);
    table_cache(const table_cache&) = delete;
    table_cache(table_cache&&) = default;
    table_cache& operator=(const table_cache&) = delete;
    table_cache& operator=(table_cache&&) = default;
    ~table_cache();

    // Create an iterator
    ss::future<std::unique_ptr<internal::iterator>>
    create_iterator(internal::file_id, uint64_t file_size);

    // Calls `fn` if the seek to `key` on this table would return a valid value.
    ss::future<> get(
      internal::file_id,
      uint64_t file_size,
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn);

    // Manually evict this file from the cache. There must not be any
    // open iterators or concurrent calls to `get` for this file.
    ss::future<> evict(internal::file_id);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::db
