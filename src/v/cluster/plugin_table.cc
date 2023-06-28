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

#include "cluster/types.h"
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
    _underlying.insert_or_assign(id, std::move(meta));
    run_callbacks(id);
}

void plugin_table::remove_transform(const transform_name& name) {
    auto it = _name_index.find(std::string_view(name()));
    if (it == _name_index.end()) {
        return;
    }
    transform_id id = it->second;
    vassert(
      _underlying.erase(id),
      "inconsistency: name index index had a record with name: {} id: {}",
      name,
      id);
    _name_index.erase(it);
    run_callbacks(id);
}
void plugin_table::reset_transforms(plugin_table::underlying_t snap) {
    name_index_t snap_name_index;

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
    }

    // Do the actual swap
    _underlying.clear();
    _underlying = std::move(snap);
    _name_index.clear();
    _name_index = std::move(snap_name_index);

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
