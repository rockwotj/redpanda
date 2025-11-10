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

#include "kafka/server/handlers/lookup_value_for_key.h"

#include "cluster/partition_manager.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "lsm/stm/key_index_stm.h"
#include "model/namespace.h"

#include <exception>
#include <iterator>

namespace kafka {

namespace {

lookup_value_for_key_response make_top_level_error(error_code ec) {
    lookup_value_for_key_response resp;
    resp.data.error_code = ec;
    return resp;
}

ss::future<error_code> lookup_values(
  ss::shared_ptr<lsm::key_index_stm> stm,
  const chunked_vector<bytes>& keys,
  chunked_vector<lookup_value_data>& results) {
    try {
        for (auto [key, result] : std::views::zip(keys, results)) {
            result.data = co_await stm->lookup_value(std::string_view(key));
        }
    } catch (...) {
        vlog(
          klog.warn,
          "error looking up key in index: {}",
          std::current_exception());
        co_return error_code::unknown_server_error;
    }
    co_return error_code::none;
}

ss::future<lookup_value_for_key_partition_response> lookup_values(
  request_context* ctx, const model::ktp& ktp, chunked_vector<bytes> keys) {
    lookup_value_for_key_partition_response resp{
      .partition_index = ktp.get_partition(),
    };
    auto shard = ctx->shards().shard_for(ktp);
    if (!shard) {
        resp.error_code = ctx->metadata_cache().contains(ktp.as_tn_view())
                            ? error_code::not_leader_for_partition
                            : error_code::unknown_topic_or_partition;
        co_return resp;
    }
    // Allocate the memory on this shard for the values vector. The iobufs will
    // still live on the partition shard, but that's the life we've choosen with
    // the atomic deleter on the iobuf.
    resp.values.reserve(keys.size());
    for (const auto& _ : keys) {
        resp.values.emplace_back();
    }
    resp.error_code = co_await ctx->partition_manager().invoke_on(
      *shard, [&ktp, &keys, &resp](cluster::partition_manager& pm) {
          auto partition = pm.get(ktp);
          if (!partition || !partition->is_leader()) {
              return ss::as_ready_future(error_code::not_leader_for_partition);
          }
          auto key_index_stm
            = partition->raft()->stm_manager()->get<lsm::key_index_stm>();
          if (!key_index_stm) {
              return ss::as_ready_future(error_code::invalid_topic_exception);
          }
          return lookup_values(std::move(key_index_stm), keys, resp.values);
      });
    co_return resp;
}

} // namespace

template<>
ss::future<response_ptr> lookup_value_for_key_handler::handle(
  request_context ctx, ss::smp_service_group) {
    lookup_value_for_key_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (ctx.recovery_mode_enabled()) {
        co_return co_await ctx.respond(
          make_top_level_error(error_code::policy_violation));
    }
    if (!ctx.audit()) {
        co_return co_await ctx.respond(
          make_top_level_error(error_code::broker_not_available));
    }
    if (request.data.topics.empty()) {
        co_return co_await ctx.respond(
          make_top_level_error(error_code::unknown_topic_id));
    }
    for (const auto& topic_req : request.data.topics) {
        const auto& topic = topic_req.topic;
        auto authz = ctx.authorized(security::acl_operation::read, topic);
        if (!authz) {
            co_return co_await ctx.respond(
              make_top_level_error(error_code::topic_authorization_failed));
        }
    }
    lookup_value_for_key_response top_level_resp;
    for (auto& topic_req : request.data.topics) {
        auto& topic = topic_req.topic;
        const auto& md_cache = ctx.metadata_cache();
        if (md_cache.should_reject_reads({model::kafka_namespace, topic})) {
            co_return co_await ctx.respond(
              make_top_level_error(error_code::invalid_topic_exception));
        }
        auto topic_md = md_cache.get_topic_metadata_ref(
          {model::kafka_namespace, topic});
        if (!topic_md) {
            co_return co_await ctx.respond(
              make_top_level_error(error_code::unknown_topic_or_partition));
        }
        const auto& topic_cfg = topic_md->get().get_configuration();
        if (!topic_cfg.has_key_index()) {
            co_return co_await ctx.respond(
              make_top_level_error(error_code::invalid_topic_exception));
        }
        lookup_value_for_key_topic_response resp{.topic = std::move(topic)};
        for (auto& partition_req : topic_req.partitions) {
            resp.partitions.push_back(
              co_await lookup_values(
                &ctx,
                model::ktp{resp.topic, partition_req.partition_index},
                std::move(partition_req.keys)));
        }
    }
    co_return co_await ctx.respond(std::move(top_level_resp));
}

} // namespace kafka
