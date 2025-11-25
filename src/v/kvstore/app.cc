/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kvstore/app.h"

#include "cluster/partition.h"
#include "kvstore/manager.h"
#include "kvstore/scheduler.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

namespace kvstore {

app::app(ss::sstring logger_name)
  : ssx::sharded_service_container(std::move(logger_name)) {}

app::~app() = default;

ss::future<> app::construct(
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<raft::group_manager>* group_manager,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<cloud_io::remote>* remote,
  cloud_storage_clients::bucket_name bucket) {
    co_await construct_service(
      scheduler, partition_manager, group_manager, topic_table);

    co_await construct_service(
      manager,
      ss::sharded_parameter([&remote] { return &remote->local(); }),
      bucket);
}

ss::future<> app::start() {
    // Wire up notifications before starting the scheduler, as the scheduler
    // will invoke callbacks for existing partitions when started.
    co_await wire_up_notifications();

    co_await manager.invoke_on_all([](auto& m) { return m.start(); });

    // When start is called on the scheduler, it will invoke callbacks for
    // partitions already on the local shard.
    co_await scheduler.invoke_on_all([](auto& s) { return s.start(); });
}

ss::future<> app::wire_up_notifications() {
    co_await manager.invoke_on_all([this](auto& mgr) {
        scheduler.local().on_partition_leader([&mgr](
                                                const model::ntp& ntp,
                                                model::topic_id_partition tidp,
                                                auto& partition) noexcept {
            if (partition) {
                mgr.schedule_partition(ntp, tidp, *partition);
            } else {
                mgr.unschedule_partition(ntp, tidp);
            }
        });
    });
}

ss::future<> app::stop() {
    co_await ss::async([this] { ssx::sharded_service_container::shutdown(); });
}

kvstore_manager* app::get_local_manager() { return &manager.local(); }

} // namespace kvstore
