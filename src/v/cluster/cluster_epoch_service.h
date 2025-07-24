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
template<typename Clock = ss::lowres_clock>
class cluster_epoch_service
  : public ss::peering_sharded_service<cluster_epoch_service<Clock>> {
    class raft0_state;

public:
    // TODO(cloud-topics): make these configuration knobs.
    // The amount of time to cache the current epoch before we attempt an
    // update.
    constexpr static ss::lowres_clock::duration epoch_cache_timeout = 30s;
    // The interval on which we bump our epoch.
    constexpr static ss::lowres_clock::duration epoch_bump_interval = 10min;
    // Maximum amount of time to cache the same epoch before we block on the
    // update.
    constexpr static ss::lowres_clock::duration max_same_epoch_cache_duration
      = 24 * 60min;

    cluster_epoch_service(
      model::node_id,
      ss::sharded<rpc::connection_cache>*,
      ss::sharded<partition_leaders_table>*) noexcept;
    // **For testing** support injecting a custom "remote fetch epoch" function.
    explicit cluster_epoch_service(
      ss::noncopyable_function<ss::future<int64_t>(typename Clock::duration)>
        fn) noexcept;
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
    // epoch that caused the sequence violation (ie. that you got back from
    // `get_cached_epoch`) in order to actually invalidate the cache.
    void invalidate_epoch_cache(int64_t epoch_causing_sequence_violation);

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
    // The cached epoch should be updated, but don't block on it.
    bool cache_entry_expired() const noexcept;
    // The cached epoch needs updating, and block we need to block on it.
    bool cache_entry_needs_updated() const noexcept;
    ss::future<> update_epoch();
    ss::future<int64_t> fetch_leader_epoch();

    // The currently cached epoch
    int64_t _cached_epoch{-1};
    // The last time the epoch was cached
    Clock::time_point _cached_epoch_time{Clock::time_point::min()};
    // The last time the epoch actually ratcheted forward
    Clock::time_point _epoch_updated_time{Clock::time_point::min()};

    // Mutex guarding cross shard calls and RPCs to update the cache.
    ssx::checkpoint_mutex _mu{"cluster_epoch_generator"};

    ss::gate _gate;

    ss::noncopyable_function<ss::future<int64_t>(typename Clock::duration)>
      _do_fetch_leader_epoch_fn;

    std::unique_ptr<raft0_state> _shard0_state;
};

} // namespace cluster
