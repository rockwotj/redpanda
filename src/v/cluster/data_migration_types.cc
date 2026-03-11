/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/data_migration_types.h"

#include "model/namespace.h"
#include "ssx/sformat.h"
#include "utils/to_string.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

namespace cluster::data_migrations {
namespace {
ss::sstring print_migration(const data_migration& dm) {
    return ss::visit(
      dm,
      [&](const inbound_migration& idm) {
          return ssx::sformat("{{inbound_migration: {}}}", idm);
      },
      [&](const outbound_migration& odm) {
          return ssx::sformat("{{outbound_migration: {}}}", odm);
      });
}
} // namespace

data_migration copy_migration(const data_migration& migration) {
    return std::visit(
      [](const auto& migration) { return data_migration{migration.copy()}; },
      migration);
}

const chunked_vector<inbound_topic> inbound_migration::consumer_offsets_topic{
  {.source_topic_name = model::kafka_consumer_offsets_nt}};

inbound_migration inbound_migration::copy() const {
    return inbound_migration{
      .topics = topics.copy(),
      .groups = groups.copy(),
      .auto_advance = auto_advance};
}

const chunked_vector<model::topic_namespace>
  outbound_migration::consumer_offsets_topic{model::kafka_consumer_offsets_nt};

outbound_migration outbound_migration::copy() const {
    return outbound_migration{
      .topics = topics.copy(),
      .groups = groups.copy(),
      .copy_to = copy_to,
      .auto_advance = auto_advance,
      .topic_locations = topic_locations.copy(),
    };
}

std::string_view format_as(state state) {
    switch (state) {
    case state::planned:
        return "planned";
    case state::preparing:
        return "preparing";
    case state::prepared:
        return "prepared";
    case state::executing:
        return "executing";
    case state::executed:
        return "executed";
    case state::cut_over:
        return "cut_over";
    case state::finished:
        return "finished";
    case state::canceling:
        return "canceling";
    case state::cancelled:
        return "cancelled";
    case state::deleted:
        return "deleted";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& o, state s) {
    return o << format_as(s);
}

std::string_view format_as(migrated_replica_status status) {
    switch (status) {
    case migrated_replica_status::waiting_for_rpc:
        return "waiting_for_rpc";
    case migrated_replica_status::waiting_for_controller_update:
        return "waiting_for_controller_update";
    case migrated_replica_status::can_run:
        return "can_run";
    case migrated_replica_status::done:
        return "done";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& o, migrated_replica_status status) {
    return o << format_as(status);
}

std::string_view format_as(migrated_resource_state state) {
    switch (state) {
    case migrated_resource_state::non_restricted:
        return "non_restricted";
    case migrated_resource_state::metadata_locked:
        return "restricted";
    case migrated_resource_state::read_only:
        return "read_only";
    case migrated_resource_state::create_only:
        return "create_only";
    case migrated_resource_state::fully_blocked:
        return "fully_blocked";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& o, migrated_resource_state state) {
    return o << format_as(state);
}

fmt::iterator inbound_topic::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{source_topic_name: {}, alias: {}, cloud_storage_location: {}}}",
      source_topic_name,
      alias,
      cloud_storage_location);
}

fmt::iterator cloud_storage_location::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{hint: {}}}", hint);
}

fmt::iterator copy_target::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{bucket: {}}}", bucket);
}

fmt::iterator topic_location::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{remote_topic: {}, location: {}}}", remote_topic, location);
}

fmt::iterator inbound_migration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topics: {}, consumer_groups: {}, auto_advance: {}}}",
      fmt::join(topics, ", "),
      fmt::join(groups, ", "),
      auto_advance);
}

fmt::iterator outbound_migration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topics: {}, consumer_groups: {}, copy_to: {}, auto_advance: {}, "
      "topic_locations: {}}}",
      fmt::join(topics, ", "),
      fmt::join(groups, ", "),
      copy_to,
      auto_advance,
      fmt::join(topic_locations, ", "));
}

fmt::iterator topic_work::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{migration: {}, sought_state: {}, info: {}}}",
      migration_id,
      sought_state,
      ss::visit(
        info,
        [&](const inbound_topic_work_info& itwi) {
            return ssx::sformat(
              "{{inbound; source: {}, cloud_storage_location: {}}}",
              itwi.source,
              itwi.cloud_storage_location);
        },
        [&](const outbound_topic_work_info& otwi) {
            return ssx::sformat("{{outbound; copy_to: {}}}", otwi.copy_to);
        }));
}

fmt::iterator migration_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, migration: {}, state: {}, created_timestamp: {}, "
      "completed_timestamp: {}, revision_id: {}}}",
      id,
      print_migration(migration),
      state,
      created_timestamp,
      completed_timestamp,
      revision_id);
}

fmt::iterator data_migration_ntp_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp: {}, migration: {}, sought_state: {}}}",
      ntp,
      migration,
      state);
}

fmt::iterator
create_migration_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, migration: {}, op_timestamp: {}, "
      "fill_outbound_topic_locations: {}}}",
      id,
      print_migration(migration),
      op_timestamp,
      fill_outbound_topic_locations);
}

fmt::iterator
update_migration_state_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, requested_state: {}, op_timestamp: {}}}",
      id,
      requested_state,
      op_timestamp);
}

fmt::iterator
remove_migration_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id: {}}}", id);
}

fmt::iterator
create_migration_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{migration: {}}}", print_migration(migration));
}

fmt::iterator create_migration_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id: {}, error_code: {}}}", id, ec);
}

fmt::iterator
update_migration_state_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id: {}, state: {}}}", id, state);
}

fmt::iterator
update_migration_state_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error_code: {}}}", ec);
}

fmt::iterator
remove_migration_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id: {}}}", id);
}

fmt::iterator remove_migration_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error_code: {}}}", ec);
}

fmt::iterator
check_ntp_states_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{sought_states: {}}}", sought_states);
}

fmt::iterator
check_ntp_states_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{actual_states: {}}}", actual_states);
}

fmt::iterator entities_status::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{groups: {}}}",
      fmt::join(
        std::views::transform(
          groups, [](const group_offsets& g) { return g.group_id; }),
        ", "));
}

} // namespace cluster::data_migrations
