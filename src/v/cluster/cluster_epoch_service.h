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
#include "cluster/partition_leaders_table.h"
#include "raft/consensus.h"
#include "raft/group_manager.h"
#include "rpc/connection_cache.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/distributed.hh>
#include <seastar/core/gate.hh>

namespace cluster {

class controller_stm;

// A sharded service on every core that is responsible for returning the
// cluster's epoch.
//
// The cluster epoch is a monotonically increasing value that is currently used
// in the Cloud Topics's L0 implementation.
class cluster_epoch_service
  : public ss::peering_sharded_service<cluster_epoch_service> {
    class raft0_state;

public:
    cluster_epoch_service(
      model::node_id,
      ss::sharded<rpc::connection_cache>*,
      ss::sharded<partition_leaders_table>*) noexcept;
    cluster_epoch_service(const cluster_epoch_service&) = delete;
    cluster_epoch_service(cluster_epoch_service&&) = delete;
    cluster_epoch_service& operator=(const cluster_epoch_service&) = delete;
    cluster_epoch_service& operator=(cluster_epoch_service&&) = delete;
    ~cluster_epoch_service() noexcept;

    ss::future<> start();
    ss::future<> stop();

    // Invalidate any caching that may (or may not) be going on of the current
    // epoch.
    //
    // This should only be used if another part of the system actually observed
    // a higher epoch, otherwise this will needlessly invalidate the cache.
    //
    // To ensure we are not continually invaliding epochs, you must pass the
    // epoch you observed in order to actually invalidate the cache.
    void invalidate_epoch_cache(int64_t observed_epoch);

    // Returns the current epoch (with caching) for the cluster.
    //
    // May be called on any shard.
    ss::future<int64_t> get_cached_epoch();

    // Returns the current epoch for the cluster.
    //
    // Returns an std::nullopt if not currently the leader of raft0.
    //
    // REQUIRES: Must only be called on shard0.
    ss::future<std::optional<int64_t>> get_current_epoch();

    // Set the controller stm instance used to generate
    // the cluster epoch from.
    //
    // Also sets the raft_group manager, which is used to subscribe to
    // leadership changes on raft0.
    //
    // Must only be set on shard0
    void set_raft0(
      ss::lw_shared_ptr<raft::consensus> raft0,
      ss::sharded<controller_stm>& controller_stm,
      ss::sharded<raft::group_manager>& raft_manager) noexcept;

private:
    bool cache_entry_expired() const noexcept;
    ss::future<int64_t> fetch_leader_epoch();
    ss::future<int64_t>
    do_fetch_leader_epoch(ss::lowres_clock::duration timeout);

    int64_t _cached_epoch{-1};
    ss::lowres_clock::time_point _cached_epoch_time{
      ss::lowres_clock::time_point::min()};
    ssx::checkpoint_mutex _mu{"cluster_epoch_generator"};
    ss::gate _gate;
    model::node_id _self;
    ss::sharded<rpc::connection_cache>* _rpc_conn;
    ss::sharded<partition_leaders_table>* _leaders;

    std::unique_ptr<raft0_state> _shard0_state;
};

} // namespace cluster
