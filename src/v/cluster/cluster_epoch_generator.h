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
class cluster_epoch_generator
  : public ss::peering_sharded_service<cluster_epoch_generator> {
    struct shard0_state;

public:
    cluster_epoch_generator() noexcept;
    cluster_epoch_generator(const cluster_epoch_generator&) = delete;
    cluster_epoch_generator(cluster_epoch_generator&&) = delete;
    cluster_epoch_generator& operator=(const cluster_epoch_generator&) = delete;
    cluster_epoch_generator& operator=(cluster_epoch_generator&&) = delete;
    ~cluster_epoch_generator() noexcept;

    ss::future<> start();
    ss::future<> stop();

    // Returns the current epoch for the cluster.
    ss::future<int64_t> current_epoch();

    // Set the controller stm instance used to generate the cluster epoch
    // from.
    //
    // Must only be set on shard0
    void set_raft0(ss::sharded<controller_stm>&) noexcept;

private:
    ss::future<int64_t> get_current_epoch();

    bool cache_entry_expired() const noexcept;

    int64_t _cached_epoch{-1};
    ss::lowres_clock::time_point _cached_epoch_time{
      ss::lowres_clock::time_point::min()};
    ssx::checkpoint_mutex _mu{"cluster_epoch_generator"};
    ss::gate _gate;

    std::unique_ptr<shard0_state> _shard0_state;
};

} // namespace cluster
