/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/plugin_table.h"
namespace cluster {
/**
 * The plugin backend is responsible for dispatching controller updates into the
 * `plugin` table.
 */
class plugin_backend {
public:
    explicit plugin_backend(ss::sharded<plugin_table>*);

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    ss::future<std::error_code> apply_update(model::record_batch);
    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::plugin_update;
    }

private:
    static constexpr auto accepted_commands = make_commands_list<
      transform_update_cmd,
      transform_remove_cmd,
      transform_partition_failed_cmd>();

    ss::sharded<plugin_table>* _table;
};
} // namespace cluster
