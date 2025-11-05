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

#include "cluster/state_machine_registry.h"

#pragma once

namespace lsm {

class key_index_stm_factory : public cluster::state_machine_factory {
public:
    explicit key_index_stm_factory(std::filesystem::path path)
      : _path(std::move(path)) {}

    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft,
      const cluster::stm_instance_config&) final;

private:
    std::filesystem::path _path;
};

} // namespace lsm
