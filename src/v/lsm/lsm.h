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
#include "lsm/core/internal/iterator.h"
#include "lsm/db/impl.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm {

namespace db {
class impl;
}

class iterator;

// Options for the database.
struct options {};

// A LSM tree database. Note that this database does *not* have a WAL.
class database {
public:
    explicit database(std::unique_ptr<db::impl> impl);
    database(const database&) = delete;
    database(database&&) = default;
    database& operator=(const database&) = delete;
    database& operator=(database&&) = default;
    ~database() noexcept;

    // Open the database.
    static ss::future<database> open(options, std::unique_ptr<io::persistence>);

    // Close the database, no more operations should happen to the database at
    // this point, and all iterators should be closed before calling this
    // method.
    //
    // This *must* be called before destroying the database.
    ss::future<> close();

    // The maximum offset that has been persisted to durable storage.
    model::offset max_persisted_offset() const;

    // Apply a record batch to the database.
    //
    // Caveats:
    // - Tombstone records are treated as deletes
    // - Keys over 32KiB are skipped
    // - Records with null keys are skipped
    ss::future<> apply(model::record_batch);

    // Lookup a value in the database
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database.
    ss::future<iterator> create_iterator();

private:
    std::unique_ptr<db::impl> _impl;
};

namespace internal {
class iterator;
}

// An iterator over the contents of the database.
class iterator {
public:
    explicit iterator(std::unique_ptr<internal::iterator> impl);
    iterator(const iterator&) = delete;
    iterator(iterator&&) = default;
    iterator& operator=(const iterator&) = delete;
    iterator& operator=(iterator&&) = default;
    ~iterator() noexcept;
    // An iterator is either positioned at a key/value pair, or
    // not valid. This method returns true iff the iterator is valid.
    bool valid() const;
    // Position at the first key in the source. The iterator is valid()
    // after this call iff the source is not empty.
    ss::future<> seek_to_first();

    // Position at the last key in the source. The iterator is
    // valid() after this call iff the source is not empty.
    ss::future<> seek_to_last();

    // Position at the first key in the source that is at or past target.
    // The iterator is valid() after this call iff the source contains
    // an entry that comes at or past target.
    ss::future<> seek(std::string_view target);

    // Moves to the next entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: valid()
    ss::future<> next();

    // Moves to the previous entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: valid()
    ss::future<> prev();

    // Return the key for the current entry. The returned value is only valid
    // until the iterator is moved.
    // REQUIRES: valid()
    std::string_view key();

    // Return the value for the current entry.
    // REQUIRES: valid()
    iobuf value();

private:
    std::unique_ptr<internal::iterator> _impl;
};

} // namespace lsm
