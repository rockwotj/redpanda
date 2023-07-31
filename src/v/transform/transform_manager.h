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

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "ssx/work_queue.h"
#include "transform/fwd.h"
#include "transform/io.h"
#include "transform/transform_processor.h"
#include "wasm/fwd.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>

namespace transform {

using ntp_leader = ss::bool_class<struct is_ntp_leader>;

// This allows reading the existing transforms by input topic or by ID.
//
// This allows us to swap out the data source for plugins in tests easily.
class plugin_registry {
public:
    plugin_registry() = default;
    plugin_registry(const plugin_registry&) = delete;
    plugin_registry& operator=(const plugin_registry&) = delete;
    plugin_registry(plugin_registry&&) = default;
    plugin_registry& operator=(plugin_registry&&) = default;
    virtual ~plugin_registry() = default;

    // Get all the partitions that are leaders and this shard is responsible
    // for
    virtual absl::flat_hash_set<model::partition_id>
      get_leader_partitions(model::topic_namespace_view) const = 0;

    virtual absl::
      flat_hash_map<cluster::transform_id, cluster::transform_metadata>
        lookup_by_input_topic(model::topic_namespace_view) const = 0;

    virtual std::optional<cluster::transform_metadata>
      lookup_by_id(cluster::transform_id) const = 0;

    virtual void report_error(
      cluster::transform_id,
      model::partition_id,
      cluster::transform_metadata) const
      = 0;

    virtual ss::future<std::optional<iobuf>>
      fetch_binary(model::offset, std::chrono::milliseconds) const = 0;
};

class processor_table;

// transform manager is responsible for managing the lifetime of a processor and
// starting/stopping processors when various lifecycle events happen in the
// system, such as leadership changes, or deployments of new transforms.
//
// There is a manager per core and it only handles the transforms where the
// transform's source ntp is leader on the same shard.
//
// Internally, the manager operates on a single queue and lifecycle changes
// cannot proceed concurrently, this way we don't have to try and juggle the
// futures if a wasm::engine is starting up and a request comes in to tear it
// down, we'll just handle them in the order they where submitted to the
// manager. Note that it maybe possible to allow for **each** processor to have
// it's own queue in the manager, but until it's proven to be required, a per
// shard queue is used.
class manager {
public:
    manager(
      wasm::runtime*,
      std::unique_ptr<plugin_registry>,
      std::unique_ptr<source::factory>,
      std::unique_ptr<sink::factory>);
    manager(const manager&) = delete;
    manager& operator=(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;
    ~manager();

    ss::future<> start();
    ss::future<> stop();

    void on_leadership_change(model::ntp, ntp_leader);
    void on_plugin_change(cluster::transform_id);
    void on_plugin_error(
      cluster::transform_id, model::partition_id, cluster::transform_metadata);

private:
    void
    attempt_start_processor(model::ntp, cluster::transform_id, size_t attempts);

private:
    // All these private methods must be call "on" the queue.
    ss::future<> handle_leadership_change(model::ntp, ntp_leader);
    ss::future<> handle_plugin_change(cluster::transform_id);
    ss::future<> handle_plugin_error(
      cluster::transform_id, model::partition_id, cluster::transform_metadata);
    ss::future<> do_attempt_start_processor(
      model::ntp, cluster::transform_id, size_t attempts);
    ss::future<> start_processor(
      model::ntp,
      cluster::transform_id,
      cluster::transform_metadata,
      size_t attempts);
    wasm::transform_probe*
    get_or_create_probe(cluster::transform_id, const cluster::transform_name&);
    void erase_probe(cluster::transform_id);

    wasm::runtime* _runtime;
    absl::flat_hash_map<
      cluster::transform_id,
      std::unique_ptr<wasm::transform_probe>>
      _probes;
    std::unique_ptr<plugin_registry> _registry;
    std::unique_ptr<processor_table> _processors;
    ssx::work_queue _queue;
    std::unique_ptr<source::factory> _source_factory;
    std::unique_ptr<sink::factory> _sink_factory;
};
} // namespace transform
