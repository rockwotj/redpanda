/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/property.h"
#include "seastarx.h"
#include "utils/bottomless_token_bucket.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <optional>

namespace kafka {

/// Represents a homogenous pair of values that correspond to
/// ingress and egress side of the same entity
template<class T>
struct ingress_egress_state {
    T in;
    T eg;
};

class snc_quota_manager
  : public ss::peering_sharded_service<snc_quota_manager> {
public:
    using clock = ss::lowres_clock;
    using quota_t = bottomless_token_bucket::quota_t;

    snc_quota_manager();
    snc_quota_manager(const snc_quota_manager&) = delete;
    snc_quota_manager& operator=(const snc_quota_manager&) = delete;
    snc_quota_manager(snc_quota_manager&&) = delete;
    snc_quota_manager& operator=(snc_quota_manager&&) = delete;
    ~snc_quota_manager() noexcept = default;

    ss::future<> start();
    ss::future<> stop();

    /// @p enforce delay to enforce in this call
    /// @p request delay to request from the client via throttle_ms
    struct delays_t {
        clock::duration enforce{0};
        clock::duration request{0};
    };

    /// Determine throttling required by shard level TP quotas.
    /// @param connection_throttle_until (in,out) until what time the client
    /// on this conection should throttle until. If it does not, this throttling
    /// will be enforced on the next call. In: value from the last call, out:
    /// value saved until the next call.
    delays_t get_shard_delays(
      clock::time_point& connection_throttle_until,
      clock::time_point now) const;

    void record_request_tp(
      size_t request_size, clock::time_point now = clock::now()) noexcept;

    void record_response_tp(
      size_t request_size, clock::time_point now = clock::now()) noexcept;

private:
    // configuration
    config::binding<std::chrono::milliseconds> _max_kafka_throttle_delay;
    ingress_egress_state<config::binding<std::optional<quota_t>>>
      _kafka_throughput_limit_node_bps;
    config::binding<std::chrono::milliseconds> _kafka_quota_balancer_window;
    config::binding<std::chrono::milliseconds>
      _kafka_quota_balancer_node_period;
    config::binding<double> _kafka_quota_balancer_min_shard_thoughput_ratio;
    config::binding<quota_t> _kafka_quota_balancer_min_shard_thoughput_bps;

    // operational, used on each shard
    ingress_egress_state<bottomless_token_bucket> _shard_quota;
};

} // namespace kafka
