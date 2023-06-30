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
#include "plugin_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/service.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "vassert.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/variant_utils.hh>

#include <system_error>
#include <variant>

namespace cluster {

plugin_frontend::plugin_frontend(
  model::node_id s,
  partition_leaders_table* l,
  plugin_table* t,
  topic_table* tp,
  controller_stm* c,
  rpc::connection_cache* r,
  ss::abort_source* a)
  : _self(s)
  , _leaders(l)
  , _connections(r)
  , _table(t)
  , _topics(tp)
  , _abort_source(a)
  , _controller(c) {}

ss::future<std::error_code> plugin_frontend::upsert_transform(
  transform_metadata meta, model::timeout_clock::time_point timeout) {
    // The ID is looked up later.
    transform_cmd c{transform_update_cmd{transform_id(-1), std::move(meta)}};
    return do_mutation(std::move(c), timeout);
}

ss::future<std::error_code> plugin_frontend::remove_transform(
  transform_name name, model::timeout_clock::time_point timeout) {
    transform_cmd c{transform_remove_cmd{std::move(name), 0}};
    return do_mutation(std::move(c), timeout);
}

ss::future<std::error_code> plugin_frontend::do_mutation(
  transform_cmd cmd, model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders->get_leader(model::controller_ntp);
    if (!cluster_leader) {
        co_return errc::no_leader_controller;
    }
    if (*cluster_leader != _self) {
        co_return co_await dispatch_mutation_to_remote(
          *cluster_leader, std::move(cmd), timeout);
    }
    if (ss::this_shard_id() != controller_stm_shard) {
        co_return co_await container().invoke_on(
          controller_stm_shard,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [cmd = std::move(cmd), timeout](auto& service) mutable {
              return service.do_mutation(std::move(cmd), timeout);
          });
    }
    // Make sure we're up to date
    auto result = co_await _controller->quorum_write_empty_batch(timeout);
    if (!result) {
        co_return errc::not_leader_controller;
    }
    std::error_code ec = co_await do_local_mutation(
      std::move(cmd), result.value().last_offset, timeout);
    if (ec) {
        co_return ec;
    }
    // This is an optimization to reduce metadata propagation lag, we can
    // safely ignore the result as it doesn't effect correctness.
    co_await _controller->insert_linearizable_barrier(timeout).discard_result();
    co_return errc::success;
}

ss::future<std::error_code> plugin_frontend::dispatch_mutation_to_remote(
  model::node_id, transform_cmd, model::timeout_clock::time_point) {
    co_return errc::success;
}
ss::future<std::error_code> plugin_frontend::do_local_mutation(
  transform_cmd cmd,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    assign_id(&cmd, offset);
    auto ec = validate_mutation(cmd);
    if (ec) {
        return ss::make_ready_future<std::error_code>(ec);
    }
    bool throttled = std::visit(
      [this](const auto& cmd) {
          using T = std::decay_t<decltype(cmd)>;
          return _controller->throttle<T>();
      },
      cmd);
    if (throttled) {
        return ss::make_ready_future<std::error_code>(
          errc::throttling_quota_exceeded);
    }
    auto b = std::visit(
      [](auto cmd) { return serde_serialize_cmd(std::move(cmd)); },
      std::move(cmd));
    return _controller->replicate_and_wait(
      std::move(b), timeout, *_abort_source);
}

void plugin_frontend::assign_id(
  transform_cmd* cmd, model::offset latest_raft0_offset) {
    if (!std::holds_alternative<transform_update_cmd>(*cmd)) {
        return;
    }
    auto& update_cmd = std::get<transform_update_cmd>(*cmd);
    auto id = _table->find_id_by_name(update_cmd.value.name);
    if (id.has_value()) {
        update_cmd.key = id.value();
    } else {
        // Ensure uniqueness without keeping another ID around by assigning the
        // raft0 offset as the ID.
        update_cmd.key = transform_id(latest_raft0_offset());
    }
}

std::error_code plugin_frontend::validate_mutation(const transform_cmd& cmd) {
    return ss::visit(
      cmd,
      [this](const transform_update_cmd& cmd) {
          // Any mutations are allowed to change environment variables, so we
          // always need to validate those
          constexpr static size_t max_key_size = 128;
          constexpr static size_t max_value_size = 2_KiB;
          constexpr static size_t max_env_vars = 128;
          if (cmd.value.environment.size() > max_env_vars) {
              return errc::transform_invalid_environment;
          }
          for (const auto& [k, v] : cmd.value.environment) {
              if (k.find("=") != ss::sstring::npos) {
                  return errc::transform_invalid_environment;
              }
              if (k.find("REDPANDA_") == 0) {
                  return errc::transform_invalid_environment;
              }
              if (k.size() > max_key_size) {
                  return errc::transform_invalid_environment;
              }
              if (v.size() > max_value_size) {
                  return errc::transform_invalid_environment;
              }
          }

          auto existing = _table->find_by_id(cmd.key);
          if (existing.has_value()) {
              // update!
              // Only the offset pointer and environment can change.
              if (existing->name != cmd.value.name) {
                  return errc::transform_invalid_update;
              }
              if (existing->input_topic != cmd.value.input_topic) {
                  return errc::transform_invalid_update;
              }
              if (existing->output_topics != cmd.value.output_topics) {
                  return errc::transform_invalid_update;
              }
              return errc::success;
          }

          // create!
          if (cmd.value.name().empty()) {
              return errc::transform_invalid_create;
          }
          auto input_topic = _topics->get_topic_metadata(cmd.value.input_topic);
          if (!input_topic) {
              return errc::topic_not_exists;
          }
          const auto& input_config = input_topic->get_configuration();
          if (input_config.is_internal()) {
              return errc::transform_invalid_create;
          }
          // TODO: Support input read replicas?
          if (input_config.is_read_replica()) {
              return errc::transform_invalid_create;
          }
          if (cmd.value.output_topics.empty()) {
              return errc::transform_invalid_create;
          }
          constexpr static size_t max_output_topics = 1;
          if (cmd.value.output_topics.size() > max_output_topics) {
              return errc::transform_invalid_create;
          }
          for (const auto& out_name : cmd.value.output_topics) {
              auto output_topic = _topics->get_topic_metadata(out_name);
              if (!output_topic) {
                  return errc::topic_not_exists;
              }
              const auto& output_config = output_topic->get_configuration();
              if (output_config.is_internal()) {
                  return errc::transform_invalid_create;
              }
              if (output_config.is_read_replica()) {
                  return errc::transform_invalid_create;
              }
              if (
                output_config.partition_count < input_config.partition_count) {
                  // copartitioning is required
                  return errc::transform_invalid_create;
              }
          }
          return errc::success;
      },
      [this](const transform_remove_cmd& cmd) {
          auto transform = _table->find_by_name(cmd.key);
          if (!transform) {
              return errc::transform_does_not_exist;
          }
          return errc::success;
      });
}

plugin_frontend::notification_id plugin_frontend::register_for_updates(
  plugin_frontend::notification_callback cb) {
    return _table->register_for_updates(std::move(cb));
}

void plugin_frontend::unregister_for_updates(notification_id id) {
    return _table->unregister_for_updates(id);
}
std::optional<transform_metadata>
plugin_frontend::lookup_transform(transform_id id) const {
    return _table->find_by_id(id);
}

absl::flat_hash_map<transform_id, transform_metadata>
plugin_frontend::all_transforms() const {
    return _table->all_transforms();
}
} // namespace cluster
