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
#include "raft/consensus.h"
#include "raft/group_manager.h"
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
    cluster_epoch_service() noexcept;
    cluster_epoch_service(const cluster_epoch_service&) = delete;
    cluster_epoch_service(cluster_epoch_service&&) = delete;
    cluster_epoch_service& operator=(const cluster_epoch_service&) = delete;
    cluster_epoch_service& operator=(cluster_epoch_service&&) = delete;
    ~cluster_epoch_service() noexcept;

    ss::future<> start();
    ss::future<> stop();

    // Returns the current epoch for the cluster.
    ss::future<int64_t> current_epoch();

    // Invalidate any caching that may (or may not) be going on of the current
    // epoch.
    void invalidate_epoch_cache();

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
    ss::future<std::optional<int64_t>> get_current_epoch();

    bool cache_entry_expired() const noexcept;

    int64_t _cached_epoch{-1};
    ss::lowres_clock::time_point _cached_epoch_time{
      ss::lowres_clock::time_point::min()};
    ssx::checkpoint_mutex _mu{"cluster_epoch_generator"};
    ss::gate _gate;

    std::unique_ptr<raft0_state> _shard0_state;
};

} // namespace cluster
