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
#include "base/format_to.h"
#include "base/seastarx.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "storage/node.h"
#include "utils/absl_sstring_hash.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
namespace kafka {

/**
 * Simple class responsible for tracking and throttling  producers thar are
 * writing data to Iceberg enabled topics.
 *
 * Throttle manager has two parts one is the shard local map used to quickly
 * update producer state when handling the producer requests. The state from the
 * local maps is eventually merged in a global map on shard 0. The shard 0
 * global map is queried only when Iceberg capabilites are enabled and the
 * Iceberg backlog is over the threshold making x-shard access very rare.
 */
class datalake_throttle_manager
  : public ss::peering_sharded_service<datalake_throttle_manager> {
public:
    struct status {
        bool max_shares_assigned{false};
        size_t total_translation_backlog{0};

        status operator+(const status& o) const;

        fmt::iterator format_to(fmt::iterator) const;
    };

    using clock_type = ss::lowres_clock;
    /**
     * The function should return a number reflecting the current backlog of
     * Iceberg translation on a shard.
     */
    using status_provider_fn = ss::noncopyable_function<ss::future<status>()>;

    datalake_throttle_manager(
      status_provider_fn,
      ss::sharded<storage::node>&,
      config::binding<std::chrono::milliseconds>,
      config::binding<std::chrono::milliseconds>,
      config::binding<std::optional<double>>);

    void start();
    ss::future<> stop();

    // This method can be called on any shard it will update the local producer
    // state so it is marked as the datalake topic related producer.
    void mark_datalake_producer(
      const std::optional<std::string_view>& producer_id,
      clock_type::time_point now = clock_type::now());

    // This method can be called on any shard it will return the throttle time
    // if the Iceberg translation backlog limit is breached.
    ss::future<std::chrono::milliseconds>
    maybe_throttle_producer(std::optional<std::string_view> client_id);

private:
    ss::future<> gc_and_update_global_producers_map();
    struct producer_state {
        clock_type::time_point last_datalake_produce = clock_type::now();
    };
    using producers_map_t
      = chunked_hash_map<ss::sstring, producer_state, sstring_hash, sstring_eq>;
    static void
    merge_producer_maps(producers_map_t& target, const producers_map_t& source);

    // must be called only on shard 0
    std::chrono::milliseconds
    get_producer_throttle(const std::optional<std::string_view>&);

    std::chrono::milliseconds calculate_throttle() const;

    void setup_metrics();

    bool needs_throttling() const;

    status_provider_fn _shard_status_provider;
    ss::sharded<storage::node>& _storage_node;
    config::binding<std::chrono::milliseconds> _producer_gc_threshold;
    config::binding<std::chrono::milliseconds> _max_kafka_throttle;
    config::binding<std::optional<double>> _backlog_size_throttling_ratio;

    // Timestamp marking a last time when the Iceberg translation reported no
    // issues. This timestamp is used to calculate the throttle, the longer the
    // issue persist the bigger the throttle.
    ss::lowres_clock::time_point _last_no_issues_timestamp = clock_type::now();
    // global producers are maintained only on shard 0.
    producers_map_t _global_producers;
    // backlog status, calculated on shard 0 but updated on every shard
    status _translation_status;
    // shard local producers map used to quickly update the state of producer.
    // The global map is updated by the background task.
    producers_map_t _shard_local_producers;
    ss::timer<clock_type> _update_timer;
    size_t _total_throttle{0};
    size_t _throttled_requests{0};
    metrics::internal_metric_groups _metrics;
    storage::node::notification_id _storage_node_notification;
    // The disk space info is set by the notification on shard 0 and then
    // propagated to other shards in the update task.
    std::optional<storage::node::disk_space_info> _disk_space_info;
    ss::gate _gate;
};
} // namespace kafka
