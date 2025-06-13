/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "cluster/state_machine_registry.h"
#include "raft/consensus.h"
#include "raft/state_machine.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/coroutine/parallel_for_each.hh>

#include <gtest/gtest.h>

#include <functional>
#include <string_view>

static ss::logger kv_log{"pandadb"};

class pandadb : public raft::state_machine_base {
public:
    static constexpr const std::string_view name = "pandadb";

    explicit pandadb(raft::consensus* consensus)
      : _consensus(consensus) {
        std::ignore = _consensus;
        // This is how you truncate the log at an offset.
        // std::ignore = _consensus->write_snapshot(
        // raft::write_snapshot_cfg(model::offset(0), iobuf{}));
    }

    ss::future<> start() final { co_return; }

    ss::future<>
    apply(const model::record_batch& batch, const ssx::semaphore_units&) final {
        std::ignore = batch;
        co_return;
    }

    ss::future<> stop() final { co_await raft::state_machine_base::stop(); }

    ss::future<> apply_raft_snapshot(const iobuf&) final { co_return; }

    ss::future<iobuf>
    take_snapshot(model::offset /*last_included_offset*/) final {
        co_return iobuf{};
    }

    size_t get_local_state_size() const final { return 0; };

    ss::future<> remove_local_state() final { co_return; }

    raft::snapshot_at_offset_supported
    supports_snapshot_at_offset() const final {
        return raft::snapshot_at_offset_supported::no;
    }

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

private:
    raft::consensus* _consensus;
};

class PandaDBFixture : public raft::raft_fixture {
public:
    ss::future<> create_group(size_t number_of_nodes) {
        std::vector<std::reference_wrapper<raft::raft_node_instance>> nodes;
        nodes.reserve(number_of_nodes);
        for (size_t id = 0; id < number_of_nodes; ++id) {
            nodes.emplace_back(add_node(
              model::node_id(static_cast<int32_t>(id)), model::revision_id{0}));
        }

        co_await ss::coroutine::parallel_for_each(nodes, [this](auto node) {
            raft::state_machine_manager_builder builder;
            builder.create_stm<pandadb>(node.get().raft().get());
            return node.get().init_and_start(all_vnodes(), std::move(builder));
        });
    }
};

TEST_F_CORO(PandaDBFixture, Setup) {
    co_await create_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    ASSERT_TRUE_CORO(all_ids().contains(leader_id));
    auto& leader_node = node(leader_id);
    std::ignore = leader_node;
}
