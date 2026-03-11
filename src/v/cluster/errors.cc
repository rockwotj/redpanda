/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/errc.h"

#include <iostream>

namespace cluster {
std::string_view format_as(errc err) {
    switch (err) {
    case errc::success:
        return "cluster::errc::success";
    case errc::notification_wait_timeout:
        return "cluster::errc::notification_wait_timeout";
    case errc::topic_invalid_partitions:
        return "cluster::errc::topic_invalid_partitions";
    case errc::topic_invalid_replication_factor:
        return "cluster::errc::topic_invalid_replication_factor";
    case errc::topic_invalid_config:
        return "cluster::errc::topic_invalid_config";
    case errc::not_leader_controller:
        return "cluster::errc::not_leader_controller";
    case errc::topic_already_exists:
        return "cluster::errc::topic_already_exists";
    case errc::replication_error:
        return "cluster::errc::replication_error";
    case errc::shutting_down:
        return "cluster::errc::shutting_down";
    case errc::no_leader_controller:
        return "cluster::errc::no_leader_controller";
    case errc::join_request_dispatch_error:
        return "cluster::errc::join_request_dispatch_error";
    case errc::seed_servers_exhausted:
        return "cluster::errc::seed_servers_exhausted";
    case errc::auto_create_topics_exception:
        return "cluster::errc::auto_create_topics_exception";
    case errc::timeout:
        return "cluster::errc::timeout";
    case errc::topic_not_exists:
        return "cluster::errc::topic_not_exists";
    case errc::invalid_topic_name:
        return "cluster::errc::invalid_topic_name";
    case errc::partition_not_exists:
        return "cluster::errc::partition_not_exists";
    case errc::not_leader:
        return "cluster::errc::not_leader";
    case errc::partition_already_exists:
        return "cluster::errc::partition_already_exists";
    case errc::waiting_for_recovery:
        return "cluster::errc::waiting_for_recovery";
    case errc::waiting_for_reconfiguration_finish:
        return "cluster::errc::waiting_for_reconfiguration_finish";
    case errc::update_in_progress:
        return "cluster::errc::update_in_progress";
    case errc::user_exists:
        return "cluster::errc::user_exists";
    case errc::user_does_not_exist:
        return "cluster::errc::user_does_not_exist";
    case errc::invalid_producer_epoch:
        return "cluster::errc::invalid_producer_epoch";
    case errc::sequence_out_of_order:
        return "cluster::errc::sequence_out_of_order";
    case errc::generic_tx_error:
        return "cluster::errc::generic_tx_error";
    case errc::node_does_not_exists:
        return "cluster::errc::node_does_not_exists";
    case errc::invalid_node_operation:
        return "cluster::errc::invalid_node_operation";
    case errc::invalid_configuration_update:
        return "cluster::errc::invalid_configuration_update";
    case errc::topic_operation_error:
        return "cluster::errc::topic_operation_error";
    case errc::no_eligible_allocation_nodes:
        return "cluster::errc::no_eligible_allocation_nodes";
    case errc::allocation_error:
        return "cluster::errc::allocation_error";
    case errc::partition_configuration_revision_not_updated:
        return "cluster::errc::partition_configuration_revision_not_updated";
    case errc::partition_configuration_in_joint_mode:
        return "cluster::errc::partition_configuration_in_joint_mode";
    case errc::partition_configuration_leader_config_not_committed:
        return "cluster::errc::partition_configuration_leader_config_not_"
               "committed";
    case errc::partition_configuration_differs:
        return "cluster::errc::partition_configuration_differs";
    case errc::data_policy_already_exists:
        return "cluster::errc::data_policy_already_exists";
    case errc::data_policy_not_exists:
        return "cluster::errc::data_policy_not_exists";
    case errc::source_topic_not_exists:
        return "cluster::errc::source_topic_not_exists";
    case errc::source_topic_still_in_use:
        return "cluster::errc::source_topic_still_in_use";
    case errc::waiting_for_partition_shutdown:
        return "cluster::errc::waiting_for_partition_shutdown";
    case errc::error_collecting_health_report:
        return "cluster::errc::error_collecting_health_report";
    case errc::leadership_changed:
        return "cluster::errc::leadership_changed";
    case errc::feature_disabled:
        return "cluster::errc::feature_disabled";
    case errc::invalid_request:
        return "cluster::errc::invalid_request";
    case errc::no_update_in_progress:
        return "cluster::errc::no_update_in_progress";
    case errc::unknown_update_interruption_error:
        return "cluster::errc::unknown_update_interruption_error";
    case errc::throttling_quota_exceeded:
        return "cluster::errc::throttling_quota_exceeded";
    case errc::cluster_already_exists:
        return "cluster::errc::cluster_already_exists";
    case errc::no_partition_assignments:
        return "cluster::errc::no_partition_assignments";
    case errc::failed_to_create_partition:
        return "cluster::errc::failed_to_create_partition";
    case errc::partition_operation_failed:
        return "cluster::errc::partition_operation_failed";
    case errc::transform_does_not_exist:
        return "cluster::errc::transform_does_not_exist";
    case errc::transform_invalid_update:
        return "cluster::errc::transform_invalid_update";
    case errc::transform_invalid_create:
        return "cluster::errc::transform_invalid_create";
    case errc::transform_invalid_source:
        return "cluster::errc::transform_invalid_source";
    case errc::transform_invalid_environment:
        return "cluster::errc::transform_invalid_environment";
    case errc::trackable_keys_limit_exceeded:
        return "cluster::errc::trackable_keys_limit_exceeded";
    case errc::topic_disabled:
        return "cluster::errc::topic_disabled";
    case errc::partition_disabled:
        return "cluster::errc::partition_disabled";
    case errc::invalid_partition_operation:
        return "cluster::errc::invalid_partition_operation";
    case errc::concurrent_modification_error:
        return "cluster::errc::concurrent_modification_error";
    case errc::transform_count_limit_exceeded:
        return "cluster::errc::transform_count_limit_exceeded";
    case errc::role_exists:
        return "cluster::errc::role_exists";
    case errc::role_does_not_exist:
        return "cluster::errc::role_does_not_exist";
    case errc::waiting_for_shard_placement_update:
        return "cluster::errc::waiting_for_shard_placement_update";
    case errc::topic_invalid_partitions_core_limit:
        return "cluster::errc::topic_invalid_partitions_core_limit";
    case errc::topic_invalid_partitions_memory_limit:
        return "cluster::errc::topic_invalid_partitions_memory_limit";
    case errc::topic_invalid_partitions_fd_limit:
        return "cluster::errc::topic_invalid_partitions_fd_limit";
    case errc::topic_invalid_partitions_decreased:
        return "cluster::errc::topic_invalid_partitions_decreased";
    case errc::producer_ids_vcluster_limit_exceeded:
        return "cluster::errc::producer_ids_vcluster_limit_exceeded";
    case errc::validation_of_recovery_topic_failed:
        return "cluster::errc::validation_of_recovery_topic_failed";
    case errc::replica_does_not_exist:
        return "cluster::errc::replica_does_not_exist";
    case errc::invalid_data_migration_state:
        return "cluster::errc::invalid_data_migration_state";
    case errc::data_migration_not_exists:
        return "cluster::errc::data_migration_not_exists";
    case errc::data_migration_already_exists:
        return "cluster::errc::data_migration_already_exists";
    case errc::data_migration_invalid_resources:
        return "cluster::errc::data_migration_invalid_resources";
    case errc::data_migration_invalid_definition:
        return "cluster::errc::data_migration_invalid_definition";
    case errc::data_migrations_disabled:
        return "cluster::errc::data_migrations_disabled";
    case errc::resource_is_being_migrated:
        return "cluster::errc::resource_is_being_migrated";
    case errc::invalid_target_node_id:
        return "cluster::errc::invalid_target_node_id";
    case errc::topic_id_already_exists:
        return "cluster::errc::topic_id_already_exists";
    case errc::feature_sanctioned:
        return "cluster::errc::feature_sanctioned";
    }
}

std::ostream& operator<<(std::ostream& o, errc err) {
    return o << format_as(err);
}
} // namespace cluster
