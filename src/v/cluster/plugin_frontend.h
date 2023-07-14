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
#include "cluster/commands.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/plugin_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <system_error>
#include <variant>

#pragma once

namespace cluster {

/**
 * The plugin frontend takes care of all (WASM) plugin related functionality and
 * servicing requests from the rest of the cluster.
 *
 * Instance per core.
 */
class plugin_frontend : public ss::peering_sharded_service<plugin_frontend> {
private:
    using transform_cmd = std::variant<
      transform_update_cmd,
      transform_partition_failed_cmd,
      transform_remove_cmd>;

public:
    // The plugin frontend takes the corresponding services for it's core.
    plugin_frontend(
      model::node_id,
      partition_leaders_table*,
      plugin_table*,
      topic_table*,
      controller_stm*,
      rpc::connection_cache*,
      ss::abort_source*);

    using notification_id = plugin_table::notification_id;
    using notification_callback = plugin_table::notification_callback;
    struct mutation_result {
        // The UUID of the modified transform, mostly useful for cleanup
        // of wasm_binaries.
        // See transform_metadata::uuid
        uuid_t uuid;
        errc ec;
    };

    // Create or update a transform by name.
    ss::future<errc>
      upsert_transform(transform_metadata, model::timeout_clock::time_point);

    // Fail a partition of a transform
    ss::future<errc> fail_transform_partition(
      failed_transform_partition, model::timeout_clock::time_point);

    // Remove a transform by name.
    ss::future<mutation_result>
      remove_transform(transform_name, model::timeout_clock::time_point);

    // Register for updates going forward.
    //
    // This is a core local operation, you can register for updates to the
    // transforms table. If you want to lookup the transform you can then do
    // that in the callback.
    notification_id register_for_updates(notification_callback);

    // Unregister a previously registered notification.
    void unregister_for_updates(notification_id);

    // Lookup a transform by ID. Useful when paired with registering for
    // updates.
    std::optional<transform_metadata> lookup_transform(transform_id) const;

    // Lookup transforms for input topics.
    absl::flat_hash_map<transform_id, transform_metadata>
      lookup_transforms_by_input_topic(model::topic_namespace_view) const;

    // Get a snapshot of all the transforms that currently exist.
    absl::flat_hash_map<transform_id, transform_metadata>
    all_transforms() const;

private:
    // Perform a mutation request, check if this node is the cluster leader and
    // ensuring that the right core is used for local mutations.
    ss::future<mutation_result>
      do_mutation(transform_cmd, model::timeout_clock::time_point);

    // If this node is not the leader, dispatch this mutation to `node_id` as
    // the cluster's leader.
    ss::future<mutation_result> dispatch_mutation_to_remote(
      model::node_id, transform_cmd, model::timeout_clock::duration);

    // Performs (and validates) a mutation command to be inserted into the
    // controller log.
    //
    // This must take place on the controller shard on the cluster leader to
    // ensure consistency.
    ss::future<mutation_result> do_local_mutation(
      transform_cmd, model::offset, model::timeout_clock::time_point);

    // Ensures that the mutation is valid.
    //
    // This must take place on the controller shard on the cluster leader to
    // ensure consistency.
    errc validate_mutation(const transform_cmd&);

    // Would adding this input->output transform cause a cycle?
    bool would_cause_cycle(model::topic_namespace_view, model::topic_namespace);

    // If this command is a upsert, then the ID is assigned.
    //
    // The offset supplied is the latest offset applied in raft0
    //
    // This must take place on the controller shard on the cluster leader to
    // ensure consistency.
    void assign_id(transform_cmd*, model::offset);

    // Can only be accessed on the current shard.
    model::node_id _self;
    partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    plugin_table* _table;
    topic_table* _topics;
    ss::abort_source* _abort_source;

    // is null if not on shard0
    controller_stm* _controller;
};
} // namespace cluster
