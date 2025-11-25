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

#include "model/fundamental.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cluster {
class partition_manager;
class topic_table;
} // namespace cluster

namespace raft {
class group_manager;
} // namespace raft

namespace kvstore {

class kvstore_manager;
class kvstore_scheduler;

class app : public ssx::sharded_service_container {
public:
    explicit app(ss::sstring logger_name = "kvstore/app");

    app(const app&) = delete;
    app& operator=(const app&) = delete;
    app(app&&) noexcept = delete;
    app& operator=(app&&) noexcept = delete;
    ~app();

    ss::future<> construct(
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<cloud_io::remote>*,
      cloud_storage_clients::bucket_name);

    ss::future<> start();

    // Call stop on each sharded service and call their destructors.
    ss::future<> stop();

    kvstore_manager* get_local_manager();

private:
    ss::future<> wire_up_notifications();

    ss::sharded<kvstore_scheduler> scheduler;
    ss::sharded<kvstore_manager> manager;
};

} // namespace kvstore
