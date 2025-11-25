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

#include "container/chunked_hash_map.h"
#include "kvstore/db.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"

#include <memory>

namespace cluster {
class partition;
}

namespace cloud_io {
class remote;
}

namespace kvstore {

// A manager responsible for running databases on this shard.
//
// There is a manager on each shard and they manage all the ntps where the
// leader is on this shard. They spin up the database and ensure it's running
// and getting writes applied. It can also be used to get access to the database
// to perform read/write operations.
//
// TODO: Handle topic deletion
class kvstore_manager {
public:
    kvstore_manager(cloud_io::remote*, cloud_storage_clients::bucket_name);

    // Start the manager, must be called before `schedule_partition`
    ss::future<> start();
    // Stop the manager.
    ss::future<> stop();

    // Schedule this database to start.
    void schedule_partition(
      model::ntp ntp,
      model::topic_id_partition tidp,
      ss::lw_shared_ptr<cluster::partition> p);

    // Shutdown this database if it exists.
    void unschedule_partition(model::ntp ntp, model::topic_id_partition tidp);

    // Lookup the database that lives on this shard.
    db* lookup_db(const model::ntp& ntp) {
        auto it = _dbs.find(ntp);
        if (it == _dbs.end()) {
            return nullptr;
        }
        return it->second.get();
    }

private:
    // Must be called on _queue
    ss::future<> do_schedule_partition(
      model::ntp ntp,
      model::topic_id_partition tidp,
      ss::lw_shared_ptr<cluster::partition>);

    // Must be called on _queue
    ss::future<>
    do_unschedule_partition(model::ntp ntp, model::topic_id_partition tidp);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    chunked_hash_map<model::ntp, std::unique_ptr<db>> _dbs;
    ssx::work_queue _queue;
};

} // namespace kvstore
