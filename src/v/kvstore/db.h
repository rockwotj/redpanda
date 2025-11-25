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
#include "cluster/partition.h"
#include "container/chunked_vector.h"
#include "lsm/lsm.h"
#include "model/record.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>

namespace cloud_io {
class remote;
}

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

// A wrapper around the LSM tree that translates kafka semantics into KV store
// ones.
//
// Additionally, we also apply the raft log in a background fiber, and service
// reads and writes directly.
class db {
public:
    db(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      std::filesystem::path staging_dir)
      : _remote(remote)
      , _bucket(std::move(bucket))
      , _prefix(std::move(prefix))
      , _staging_dir(std::move(staging_dir)) {}

    // Destroy the database contents
    static ss::future<> destroy(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      ss::abort_source& as);

    // Start the database
    ss::future<> start();

    // Stop the database
    ss::future<> stop();

    // Lookup a single value from the database that corresponds to the key.
    ss::future<std::optional<iobuf>> get(std::string_view key);

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
    ss::future<chunked_vector<entry>> scan(scan_parameters);

    // Apply a write batch against the database atomically.
    ss::future<write_success> write(write_batch);

private:
    // REQUIRES: holds _write_mu
    ss::future<write_success>
    check_precondition(std::string_view key, const precondition&);

    // Apply the WAL to the database in a loop.
    ss::future<> apply_loop();
    // Apply a chunk of the WAL to the database.
    ss::future<> do_apply_chunk();
    // Wait for the latest record to be applied to the database.
    ss::future<> sync();
    // Replicate the record to the WAL and wait for it to be applied.
    ss::future<> replicate(model::record_batch);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _prefix;
    std::filesystem::path _staging_dir;
    ss::gate _gate;
    ss::abort_source _as;
    mutex _write_mu{"kvstore/db"};
    model::term_id _term;
    model::offset _last_applied_offset;
    ssx::condition_variable _cond_var;
    std::optional<lsm::database> _lsm;
    ss::lw_shared_ptr<cluster::partition> _partition;
};

} // namespace kvstore
