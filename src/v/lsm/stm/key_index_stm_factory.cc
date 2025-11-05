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

#include "lsm/stm/key_index_stm_factory.h"

#include "lsm/stm/key_index_stm.h"

namespace lsm {

bool key_index_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return false;
}

void key_index_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto lsm_path = _path / std::string_view(raft->ntp().path());
    builder.create_stm<key_index_stm>(raft, lsm_path);
}

} // namespace lsm
