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

#include "lsm/lsm.h"
#include "raft/consensus.h"
#include "raft/state_machine_base.h"

#include <utility>

namespace lsm {

class key_index_stm : public raft::no_at_offset_snapshot_stm_base {
public:
    constexpr static std::string_view name = "key_index_stm";

    key_index_stm(raft::consensus* c, std::filesystem::path p)
      : _raft(c)
      , _path(std::move(p)) {
        std::ignore = _raft;
    }

    ss::future<std::optional<iobuf>> lookup_value(std::string_view key);

public:
    ss::future<> start() override;

    ss::future<> stop() override;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const override {
        return raft::stm_initial_recovery_policy::read_everything;
    }

    ss::future<> apply_raft_snapshot(const iobuf&) override { co_return; }
    ss::future<iobuf> take_raft_snapshot() override { co_return iobuf{}; }
    size_t get_local_state_size() const override;
    ss::future<> remove_local_state() override;

    ss::future<> apply(
      const model::record_batch& batch, const ssx::semaphore_units&) override;

private:
    raft::consensus* _raft;
    std::filesystem::path _path;
    std::optional<database> _db;
};

} // namespace lsm
