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

#include "cluster/fwd.h"
#include "cluster/plugin_frontend.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "kafka/client/client.h"
#include "kafka/client/fwd.h"
#include "model/metadata.h"
#include "utils/uuid.h"
#include "wasm/api.h"

#include <seastar/core/future.hh>

#include <memory>

namespace transform {

class manager;
/**
 * The transform service is responsible for intersecting the current state of
 * plugins and topics and ensuring that the corresponding wasm transform is
 * running for each partition.
 *
 * Instance on every shard.
 */
class service {
public:
    service(
      wasm::runtime* rt,
      model::node_id self,
      cluster::plugin_frontend* plugins,
      cluster::partition_leaders_table* leaders,
      cluster::partition_manager* partition_manager,
      const kafka::client::configuration&);
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = delete;
    service& operator=(service&&) = delete;
    ~service();

    /**
     * This creates the internal wasm source topic if it doesn't exist and
     * starts the listeners for updates to plugins and topics.
     */
    ss::future<> start();
    /**
     * This shuts down the service by stopping the listeners and gracefully
     * ensuring all the transform state machines are stopped.
     */
    ss::future<> stop();

    /**
     * Deploy a transform to the cluster.
     */
    ss::future<cluster::errc>
      deploy_transform(cluster::transform_metadata, iobuf);

    /**
     * Delete a transform from the cluster.
     */
    ss::future<cluster::errc> delete_transform(cluster::transform_name);

    /**
     * All the existing transforms for a cluster.
     *
     * This is eventually consistent.
     */
    std::vector<cluster::transform_metadata> list_transforms();

private:
    ss::future<> create_internal_source_topic();
    ss::future<> validate_source(cluster::transform_metadata, iobuf);
    ss::future<std::pair<uuid_t, model::offset>>
      write_source(cluster::transform_name, iobuf);
    ss::future<> write_source_tombstone(uuid_t, cluster::transform_name);

    void register_notifications();
    void unregister_notifications();

    wasm::runtime* _runtime;
    model::node_id _self;
    cluster::plugin_frontend* _plugins;
    cluster::partition_leaders_table* _leaders;
    cluster::partition_manager* _partition_manager;
    std::unique_ptr<kafka::client::client> _client;
    std::unique_ptr<manager> _manager;
    ss::gate _gate;

    cluster::notification_id_type _leader_notification_id;
    cluster::notification_id_type _partition_manage_notification_id;
    cluster::notification_id_type _partition_unmanage_notification_id;
    cluster::plugin_frontend::notification_id _plugin_notification_id;
};

} // namespace transform
