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
#include "plugin_backend.h"

#include "cluster/commands.h"
#include "cluster/logger.h"

#include <seastar/core/future.hh>

namespace cluster {

plugin_backend::plugin_backend(ss::sharded<plugin_table>* t)
  : _table(t) {}

ss::future<> plugin_backend::fill_snapshot(controller_snapshot& snap) const {
    snap.plugins.transforms = _table->local().all_transforms();
    return ss::now();
}

ss::future<>
plugin_backend::apply_snapshot(model::offset, const controller_snapshot& snap) {
    return _table->invoke_on_all([&snap](auto& table) {
        table.reset_transforms(snap.plugins.transforms);
    });
}

ss::future<std::error_code>
plugin_backend::apply_update(model::record_batch b) {
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
    co_await _table->invoke_on_all([&cmd](plugin_table& table) {
        return ss::visit(
          cmd,
          [&table](transform_update_cmd update) {
              table.upsert_transform(update.key, std::move(update.value));
          },
          [&table](transform_partition_failed_cmd cmd) {
              const auto& failure = cmd.value;
              table.fail_transform_partition(
                failure.id, failure.uuid, failure.partition_id);
          },
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [&table](const transform_remove_cmd& removal) {
              table.remove_transform(removal.key);
          });
    });
    co_return errc::success;
}

} // namespace cluster
