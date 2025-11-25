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
#include "cluster/notification.h"
#include "cluster/utils/partition_change_notifier.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
class partition;
class partition_manager;
class topic_table;
} // namespace cluster

namespace raft {
class group_manager;
}

namespace kvstore {

/*
 * The kvstore scheduler runs on each shard and is responsible for dispatching
 * notifications when partition leaders are scheduled on this shard that have a
 * kvstore enabled on them. It will ensure a job is running that will write to
 * the kvstore.
 */
class kvstore_scheduler {
public:
    // The callback to be invoked when leadership for a partition changes.
    //
    // If the partition is `nullptr`, then that means the leader is no longer on
    // this core. If the partition is not `nullptr` then the leader is now
    // living on this core. Callers should be idempotent if the callback is
    // notified twice with the same state.
    using notification_cb_t = ss::noncopyable_function<void(
      const model::ntp& ntp,
      model::topic_id_partition tidp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>>&
        partition) noexcept>;

    kvstore_scheduler(
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::topic_table>*);

    // Register for notifications to kafka namespaced partitions which have
    // a kvstore enabled. The provided callback will be invoked when
    // leadership changes on this shard for the partition. See the
    // partition_change_notifier for more details.
    //
    // This method should be called *before* start is called on the
    // kvstore_scheduler.
    //
    // The provided callback will be invoked for all
    // existing shards once `start` has been called for the
    // scheduler.
    //
    // These callbacks are invoked until the scheduler is stopped.
    void on_partition_leader(notification_cb_t) noexcept;

    // Start the scheduler. After this is invoked, all notifications
    // will be invoked with existing leadership status. All
    // `on_partition_leader` methods should be already be setup before this
    // method is called. It's not supported to register new callbacks once the
    // scheduler is started.
    ss::future<> start();

    // Stop the scheduler. After this point notifications will no longer be
    // invoked.
    ss::future<> stop();

private:
    void on_leadership_change(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      bool is_leader) noexcept;

    ss::sharded<cluster::partition_manager>* partition_manager_;
    ss::sharded<cluster::topic_table>* topic_table_;
    std::unique_ptr<cluster::partition_change_notifier> notifier_;
    notification_cb_t callback_;
    std::optional<cluster::notification_id_type> notification_;
    // In the case of a topic being deleted, we no longer have the
    // topic_id_mapping_, but we need to still emit a notification with it.
    //
    // Fix this by keeping an explicit mapping and looking it up if we can't
    // find it.
    //
    // We have to key this by ntp and not just ns_tp because we want to GC
    // entries over time.
    model::ntp_map_type<model::topic_id> topic_id_mapping_;
};

} // namespace kvstore
