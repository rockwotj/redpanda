/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "plugin_table.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "vassert.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace cluster {

absl::flat_hash_map<transform_id, transform_metadata>
plugin_table::all_transforms() const {
    return _underlying;
}
std::optional<transform_id>
plugin_table::find_id_by_name(std::string_view name) const {
    auto it = _name_index.find(name);
    if (it == _name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}
std::optional<transform_id>
plugin_table::find_id_by_name(const transform_name& name) const {
    return find_id_by_name(std::string_view(name()));
}
std::optional<transform_metadata>
plugin_table::find_by_name(const transform_name& name) const {
    return find_by_name(std::string_view(name()));
}
std::optional<transform_metadata>
plugin_table::find_by_name(std::string_view name) const {
    auto id = find_id_by_name(name);
    if (!id.has_value()) {
        return std::nullopt;
    }
    auto meta = find_by_id(id.value());
    vassert(
      meta.has_value(),
      "inconsistent name index for {} expected id {}",
      name,
      id);
    return meta;
}
std::optional<transform_metadata>
plugin_table::find_by_id(transform_id id) const {
    auto it = _underlying.find(id);
    if (it == _underlying.end()) {
        return std::nullopt;
    }
    return it->second;
}
plugin_table::underlying_t plugin_table::find_by_topic(
  const topic_index_t* index, model::topic_namespace_view tn) const {
    auto it = index->find(tn);
    if (it == index->end()) {
        return {};
    }
    underlying_t output;
    for (auto id : it->second) {
        auto meta = find_by_id(id);
        vassert(
          meta.has_value(),
          "inconsistent topic index for {} expected id {}",
          tn,
          id);
        output.emplace(id, *meta);
    }
    return output;
}
plugin_table::underlying_t
plugin_table::find_by_input_topic(model::topic_namespace_view tn) const {
    return find_by_topic(&_input_index, tn);
}
plugin_table::underlying_t
plugin_table::find_by_output_topic(model::topic_namespace_view tn) const {
    return find_by_topic(&_output_index, tn);
}

void plugin_table::upsert_transform(transform_id id, transform_metadata meta) {
    auto it = _name_index.find(meta.name());
    if (it != _name_index.end()) {
        if (it->second != id) {
            throw std::logic_error(ss::format(
              "transform meta id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              meta.name,
              it->second));
        }
    } else {
        _name_index.emplace(meta.name(), id);
    }
    // Topics cannot change over the lifetime of a transform, so we don't need
    // to worry about updates specially here, additionally duplicates are
    // allowed, the plugin_frontend asserts that there are no loops in
    // transforms.
    _input_index[meta.input_topic].emplace(id);

    for (const auto& output_topic : meta.output_topics) {
        _output_index[output_topic].emplace(id);
    }
    _underlying.insert_or_assign(id, std::move(meta));
    run_callbacks(id);
}

void plugin_table::fail_transform_partition(
  transform_id id, uuid_t uuid, model::partition_id partition_id) {
    auto meta = find_by_id(id);
    if (!meta || meta->uuid != uuid) {
        return;
    }
    meta->failed_partitions.emplace(partition_id);
    upsert_transform(id, std::move(meta).value());
}

void plugin_table::remove_transform(const transform_name& name) {
    auto name_it = _name_index.find(std::string_view(name()));
    if (name_it == _name_index.end()) {
        return;
    }
    auto id = name_it->second;
    auto it = _underlying.find(id);
    vassert(
      it != _underlying.end(),
      "inconsistency: name index index had a record with name: {} id: {}",
      name,
      id);
    const auto& meta = it->second;
    // Delete input topic index entries
    [this, &meta, id]() {
        auto input_it = _input_index.find(meta.input_topic);
        vassert(
          input_it != _input_index.end() && input_it->second.erase(id) > 0,
          "inconsistency: missing transform input index entry: {} id: {}",
          meta.input_topic,
          id);
        if (input_it->second.empty()) {
            _input_index.erase(input_it);
        }
    }();
    for (const auto& output_topic : it->second.output_topics) {
        auto output_it = _output_index.find(output_topic);
        vassert(
          output_it != _output_index.end() && output_it->second.erase(id) > 0,
          "inconsistency: missing transform output index entry: {} id: {}",
          output_topic,
          id);
        if (output_it->second.empty()) {
            _output_index.erase(output_it);
        }
    }
    _name_index.erase(name_it);
    _underlying.erase(it);
    run_callbacks(id);
}
void plugin_table::reset_transforms(plugin_table::underlying_t snap) {
    name_index_t snap_name_index;
    topic_index_t snap_input_index;
    topic_index_t snap_output_index;

    // Perform a map diff to figure out which transforms need change callbacks
    // fired.
    std::vector<transform_id> all_deletes;
    std::vector<transform_id> all_changed;
    std::vector<transform_id> all_inserted;
    for (const auto& [k, v] : _underlying) {
        auto it = snap.find(k);
        if (it == snap.end()) {
            all_deletes.push_back(k);
        } else if (v != it->second) {
            all_changed.push_back(k);
        }
        // Otherwise unchanged
    }
    for (const auto& [k, v] : snap) {
        if (!_underlying.contains(k)) {
            all_inserted.push_back(k);
        }
        // build the name index
        auto it = snap_name_index.insert({v.name, k});
        if (!it.second) {
            throw std::logic_error(ss::format(
              "transform meta id={} is attempting to use a name {} which is "
              "already registered to {}",
              k,
              v.name,
              it.first->first));
        }
        // build topic indexes
        snap_input_index[v.input_topic].emplace(k);
        for (const auto& output_topic : v.output_topics) {
            snap_output_index[output_topic].emplace(k);
        }
    }

    // Do the actual swap
    _underlying.clear();
    _underlying = std::move(snap);
    _name_index.clear();
    _name_index = std::move(snap_name_index);
    _input_index.clear();
    _input_index = std::move(snap_input_index);
    _output_index.clear();
    _output_index = std::move(snap_output_index);

    for (transform_id deleted : all_deletes) {
        run_callbacks(deleted);
    }
    for (transform_id updated : all_changed) {
        run_callbacks(updated);
    }
    for (transform_id inserted : all_inserted) {
        run_callbacks(inserted);
    }
}
plugin_table::notification_id
plugin_table::register_for_updates(plugin_table::notification_callback cb) {
    auto it = _callbacks.insert({++_latest_id, std::move(cb)});
    vassert(it.second, "invalid duplicate in callbacks");
    return _latest_id;
}

void plugin_table::unregister_for_updates(notification_id id) {
    _callbacks.erase(id);
}
void plugin_table::run_callbacks(transform_id id) {
    for (const auto& [_, cb] : _callbacks) {
        cb(id);
    }
}
} // namespace cluster
