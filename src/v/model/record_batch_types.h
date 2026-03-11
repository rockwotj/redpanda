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

#pragma once

#include "utils/named_type.h"

#include <cstdint>
#include <ostream>
#include <string_view>
#include <vector>

namespace model {
enum class record_batch_type : int8_t {
    raft_data = 1,            // raft::data
    raft_configuration = 2,   // raft::configuration
    controller = 3,           // controller::*
    kvstore = 4,              // kvstore::*
    checkpoint = 5,           // checkpoint - used to achieve linearizable reads
    topic_management_cmd = 6, // controller topic command batch type
    ghost_batch = 7,          // ghost - used to fill gaps in raft recovery
    id_allocator = 8,         // id_allocator_stm::*
    tx_prepare = 9,           // tx_prepare_batch_type
    tx_fence = 10,            // tx_fence_batch_type
    tm_update = 11,           // tm_update_batch_type
    user_management_cmd = 12, // controller user management command batch type
    acl_management_cmd = 13,  // controller acl management command batch type
    group_prepare_tx = 14,    // group_prepare_tx_batch_type
    group_commit_tx = 15,     // group_commit_tx_batch_type
    group_abort_tx = 16,      // group_abort_tx_batch_type
    node_management_cmd = 17, // controller node management
    data_policy_management_cmd = 18, // data-policy management
    archival_metadata = 19,          // archival metadata updates
    cluster_config_cmd = 20,         // cluster config deltas and status
    feature_update = 21,             // Node logical versions updates
    cluster_bootstrap_cmd = 22,      // cluster bootsrap command
    version_fence = 23,              // version fence/epoch
    tx_tm_hosted_trasactions = 24,   // tx_tm_hosted_trasactions_batch_type
    prefix_truncate = 25,            // log prefix truncation type
    plugin_update = 26,              // Wasm plugin update
    tx_registry = 27,                // tx_registry_batch_type
    cluster_recovery_cmd = 28,       // cluster recovery command
    compaction_placeholder
    = 29, // place holder for last batch in a segment that was aborted
    role_management_cmd = 30, // role management command
    client_quota = 31,        // client quota command
    data_migration_cmd = 32,  // data migration manipulation command
    group_fence_tx = 33,      // fence batch in group transactions
    partition_properties_update
    = 34, // special batch type used to update partition properties
    datalake_coordinator = 35, // datalake::coordinator::*
    ctp_placeholder = 36,      // placeholder batch type used by cloud topics
    ctp_stm_command = 37,      // ctp_stm command batch
    datalake_translation_state = 38, // maintains state for translation progress
    cluster_link = 39,               // cluster link update batches
    group_block = 40, // (un)blocks group names in a consumer offsets partition
    l1_stm = 41,      // cloud_topics::l1::*
    ct_read_replica_stm = 42, // cloud_topics::read_replica::*
    MAX = ct_read_replica_stm,
};

inline constexpr std::string_view format_as(record_batch_type bt) {
    switch (bt) {
    case record_batch_type::raft_data:
        return "batch_type::raft_data";
    case record_batch_type::raft_configuration:
        return "batch_type::raft_configuration";
    case record_batch_type::controller:
        return "batch_type::controller";
    case record_batch_type::kvstore:
        return "batch_type::kvstore";
    case record_batch_type::checkpoint:
        return "batch_type::checkpoint";
    case record_batch_type::topic_management_cmd:
        return "batch_type::topic_management_cmd";
    case record_batch_type::ghost_batch:
        return "batch_type::ghost_batch";
    case record_batch_type::id_allocator:
        return "batch_type::id_allocator";
    case record_batch_type::tx_prepare:
        return "batch_type::tx_prepare";
    case record_batch_type::tx_fence:
        return "batch_type::tx_fence";
    case record_batch_type::tm_update:
        return "batch_type::tm_update";
    case record_batch_type::user_management_cmd:
        return "batch_type::user_management_cmd";
    case record_batch_type::acl_management_cmd:
        return "batch_type::acl_management_cmd";
    case record_batch_type::group_prepare_tx:
        return "batch_type::group_prepare_tx";
    case record_batch_type::group_commit_tx:
        return "batch_type::group_commit_tx";
    case record_batch_type::group_abort_tx:
        return "batch_type::group_abort_tx";
    case record_batch_type::node_management_cmd:
        return "batch_type::node_management_cmd";
    case record_batch_type::data_policy_management_cmd:
        return "batch_type::data_policy_management_cmd";
    case record_batch_type::archival_metadata:
        return "batch_type::archival_metadata";
    case record_batch_type::cluster_config_cmd:
        return "batch_type::cluster_config_cmd";
    case record_batch_type::feature_update:
        return "batch_type::feature_update";
    case record_batch_type::cluster_bootstrap_cmd:
        return "batch_type::cluster_bootstrap_cmd";
    case record_batch_type::version_fence:
        return "batch_type::version_fence";
    case record_batch_type::tx_tm_hosted_trasactions:
        return "batch_type::tx_tm_hosted_trasactions";
    case record_batch_type::prefix_truncate:
        return "batch_type::prefix_truncate";
    case record_batch_type::plugin_update:
        return "batch_type::plugin_update";
    case record_batch_type::tx_registry:
        return "batch_type::tx_registry";
    case record_batch_type::cluster_recovery_cmd:
        return "batch_type::cluster_recovery_cmd";
    case record_batch_type::compaction_placeholder:
        return "batch_type::compaction_placeholder";
    case record_batch_type::role_management_cmd:
        return "batch_type::role_management_cmd";
    case record_batch_type::client_quota:
        return "batch_type::client_quota";
    case record_batch_type::data_migration_cmd:
        return "batch_type::data_migration_cmd";
    case record_batch_type::group_fence_tx:
        return "batch_type::group_fence_tx";
    case record_batch_type::partition_properties_update:
        return "batch_type::partition_properties_update";
    case record_batch_type::datalake_coordinator:
        return "batch_type::datalake_coordinator";
    case record_batch_type::ctp_placeholder:
        return "batch_type::ctp_placeholder";
    case record_batch_type::ctp_stm_command:
        return "batch_type::ctp_stm_command";
    case record_batch_type::datalake_translation_state:
        return "datalake_translation_state";
    case record_batch_type::cluster_link:
        return "cluster_link";
    case record_batch_type::group_block:
        return "group_block";
    case record_batch_type::l1_stm:
        return "l1_stm";
    case record_batch_type::ct_read_replica_stm:
        return "ct_read_replica_stm";
    }
    return "batch_type::unknown";
}

std::ostream& operator<<(std::ostream& o, record_batch_type bt);

// The set of batch types that may appear in a data partition that aren't
// assigned a new translated offset. When translated, such batches are given an
// offset matching the next batch of type outside this set.
//
// Put simply, batches of these types do not increment the offset that would be
// returned upon translating offsets for Kafka fetches.
inline std::vector<model::record_batch_type> offset_translator_batch_types() {
    return {
      model::record_batch_type::raft_configuration,
      model::record_batch_type::archival_metadata,
      model::record_batch_type::version_fence,
      model::record_batch_type::prefix_truncate,
      model::record_batch_type::partition_properties_update,
      model::record_batch_type::datalake_translation_state,
      model::record_batch_type::group_block,
      model::record_batch_type::ctp_stm_command};
}

} // namespace model
