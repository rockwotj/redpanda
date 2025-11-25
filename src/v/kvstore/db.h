/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/types.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <filesystem>
#include <memory>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cluster {
class partition;
} // namespace cluster

namespace kvstore {

// An entry in the kvstore.
struct entry {
    // The key for this entry. Keys must be less than 16KiB.
    ss::sstring key;
    // The value for the entry. Values must be less than 1MiB.
    iobuf value;
};

// Only write this entry if there is or is not an existing entry for this key.
struct if_exists {
    bool exists;
};

// Only write this entry if the value is this specific sha256 hash.
struct if_matches {
    // Hex encoding of a sha256
    ss::sstring sha256_hash;
};

// A precondition that must hold for the write to succeed.
using precondition = std::variant<std::nullopt_t, if_exists, if_matches>;

// A put (upsert) operation against the database with an optional precondition.
struct put {
    entry entry;
    precondition precondition;
};

// A remove (delete) operation against the database with an optional
// precondition.
struct remove {
    ss::sstring key;
    precondition precondition;
};

// An atomic batch of writes to apply to the database. The total size of this
// batch must be less than `message.max.bytes` in total.
struct write_batch {
    chunked_vector<put> puts;
    chunked_vector<remove> removals;
};

// A boolean if the write to the database succeeded. A write only fails if
// a precondition does not hold.
using write_success = ss::bool_class<struct write_success_tag>;

// Interface for a kvstore database.
//
// This is an abstract interface that can be implemented by the real database
// or mocked for testing.
class db {
public:
    db(const db&) = delete;
    db(db&&) = delete;
    db& operator=(const db&) = delete;
    db& operator=(db&&) = delete;
    virtual ~db() = default;

    // Factory method to create a database implementation.
    static std::unique_ptr<db> make(
      ss::lw_shared_ptr<cluster::partition> partition,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      std::filesystem::path staging_dir);

    // Destroy the database contents
    static ss::future<> destroy(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      ss::abort_source& as);

    // Start the database
    virtual ss::future<> start() = 0;

    // Stop the database
    virtual ss::future<> stop() = 0;

    // Lookup a single value from the database that corresponds to the key.
    virtual ss::future<std::optional<iobuf>> get(std::string_view key) = 0;

    // The parameters for scanning the database.
    struct scan_parameters {
        // The optional start key (inclusive) to begin the scan at.
        std::optional<ss::sstring> start_key;
        // the optional end key (exclusive) to end the scan at.
        std::optional<ss::sstring> end_key;
        // The limit of keys to fetch.
        uint32_t limit = 0;
    };

    // Scan for a chunk of entries from the database.
    //
    // The scan performed on a snapshot of the database.
    virtual ss::future<chunked_vector<entry>> scan(scan_parameters) = 0;

    // Apply a write batch against the database atomically.
    virtual ss::future<write_success> write(write_batch) = 0;

protected:
    db() = default;
};

} // namespace kvstore
