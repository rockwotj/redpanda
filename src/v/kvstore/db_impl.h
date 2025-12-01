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

#include "cluster/partition.h"
#include "kvstore/db.h"
#include "lsm/lsm.h"
#include "model/record.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>

namespace kvstore {

// A wrapper around the LSM tree that translates kafka semantics into KV store
// ones.
//
// Additionally, we also apply the raft log in a background fiber, and service
// reads and writes directly.
class db_impl final : public db {
public:
    db_impl(
      ss::lw_shared_ptr<cluster::partition> partition,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      std::filesystem::path staging_dir)
      : _partition(std::move(partition))
      , _remote(remote)
      , _bucket(std::move(bucket))
      , _prefix(std::move(prefix))
      , _staging_dir(std::move(staging_dir)) {}

    // Start the database
    ss::future<> start() override;

    // Stop the database
    ss::future<> stop() override;

    // Lookup a single value from the database that corresponds to the key.
    ss::future<chunked_vector<std::optional<iobuf>>>
    batch_get(const chunked_vector<ss::sstring>&) override;

    // Scan for a chunk of entries from the database.
    //
    // The scan performed on a snapshot of the database.
    ss::future<chunked_vector<entry>> scan(scan_parameters) override;

    // Apply a write batch against the database atomically.
    ss::future<write_success> write(write_batch) override;

private:
    // REQUIRES: holds _write_mu
    ss::future<write_success>
    check_precondition(std::string_view key, const precondition&);

    // Apply the WAL to the database in a loop.
    ss::future<> apply_loop();
    // Apply a chunk of the WAL to the database.
    ss::future<> do_apply_chunk();
    // Wait for the latest record to be applied to the database.
    ss::future<> sync_latest();
    // Wait for the previous term data to be applied to the database.
    ss::future<> sync_previous_term();
    // Replicate the record to the WAL and wait for it to be applied.
    ss::future<> replicate(model::record_batch);

    ss::lw_shared_ptr<cluster::partition> _partition;
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
};

} // namespace kvstore
