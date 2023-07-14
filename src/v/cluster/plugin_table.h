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
#include "model/metadata.h"
#include "utils/absl_sstring_hash.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index_container.hpp>

namespace cluster {
/**
 * The plugin table exists on every core and holds the underlying data for all
 * plugin metadata.
 */
class plugin_table {
    using underlying_t = absl::flat_hash_map<transform_id, transform_metadata>;
    using name_index_t = absl::
      flat_hash_map<ss::sstring, transform_id, sstring_hash, sstring_eq>;
    using topic_index_t = absl::flat_hash_map<
      model::topic_namespace,
      absl::flat_hash_set<transform_id>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

public:
    plugin_table() = default;
    plugin_table(const plugin_table&) = delete;
    plugin_table& operator=(const plugin_table&) = delete;
    plugin_table(plugin_table&&) = default;
    plugin_table& operator=(plugin_table&&) = default;
    ~plugin_table() = default;

    using notification_id = named_type<size_t, struct plugin_notif_id_tag>;
    using notification_callback = ss::noncopyable_function<void(transform_id)>;

    // Snapshot (copy) of all the transforms
    underlying_t all_transforms() const;

    // Lookups
    std::optional<transform_metadata> find_by_name(std::string_view) const;
    std::optional<transform_metadata> find_by_name(const transform_name&) const;
    std::optional<transform_id> find_id_by_name(std::string_view) const;
    std::optional<transform_id> find_id_by_name(const transform_name&) const;
    std::optional<transform_metadata> find_by_id(transform_id) const;
    // All the transforms that use this topic as input
    underlying_t find_by_input_topic(model::topic_namespace_view) const;
    // All the transforms that output to this topic
    underlying_t find_by_output_topic(model::topic_namespace_view) const;

    // Create or update transform metadata.
    void upsert_transform(transform_id id, transform_metadata);
    // remove transform metadata by name.
    void remove_transform(const transform_name&);
    // Mark an individual partition as failed if the UUIDs match (there isn't a
    // new revision).
    void fail_transform_partition(transform_id id, uuid_t, model::partition_id);

    void reset_transforms(underlying_t);

    notification_id register_for_updates(notification_callback);

    void unregister_for_updates(notification_id);

private:
    void run_callbacks(transform_id);
    underlying_t
    find_by_topic(const topic_index_t*, model::topic_namespace_view) const;

    name_index_t _name_index;
    topic_index_t _input_index;
    topic_index_t _output_index;
    underlying_t _underlying;
    absl::flat_hash_map<notification_id, notification_callback> _callbacks;
    notification_id _latest_id{0};
};
} // namespace cluster
