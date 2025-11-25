/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kvstore/scheduler.h"

#include "cluster/partition_manager.h"
#include "cluster/topic_configuration.h"
#include "cluster/topic_table.h"
#include "cluster/utils/partition_change_notifier_impl.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace kvstore {

kvstore_scheduler::kvstore_scheduler(
  ss::sharded<cluster::partition_manager>* pm,
  ss::sharded<raft::group_manager>* gm,
  ss::sharded<cluster::topic_table>* tt)
  : partition_manager_(pm)
  , topic_table_(tt)
  , notifier_(
      cluster::partition_change_notifier_impl::make_default(*gm, *pm, *tt)) {}

void kvstore_scheduler::on_partition_leader(notification_cb_t cb) noexcept {
    callback_ = std::move(cb);
}

ss::future<> kvstore_scheduler::start() {
    using notify_current_state
      = cluster::partition_change_notifier::notify_current_state;
    using notif_type = cluster::partition_change_notifier::notification_type;
    using partition_state = cluster::partition_change_notifier::partition_state;
    notification_ = notifier_->register_partition_notifications(
      [this](
        notif_type,
        const model::ntp& ntp,
        std::optional<partition_state> state) noexcept {
          auto is_leader = state
                             .transform([](const partition_state& state) {
                                 return state.is_leader;
                             })
                             .value_or(false);
          std::optional<cluster::topic_configuration> config
            = state
                .and_then([](partition_state state) {
                    return std::move(state.topic_cfg);
                })
                .or_else([this, &ntp] {
                    return topic_table_->local().get_topic_cfg(
                      {ntp.ns, ntp.tp.topic});
                });
          if (!config || !config->tp_id) {
              auto it = topic_id_mapping_.find(ntp);
              if (it == topic_id_mapping_.end()) {
                  // This can happen if a topic is deleted and it wasn't a
                  // kvstore topic.
                  return;
              }
              // Always emit these even if they are not kvstore topics (we can't
              // know), because it means the topic was deleted.
              model::topic_id_partition tidp{it->second, ntp.tp.partition};
              topic_id_mapping_.erase(it);
              on_leadership_change(ntp, tidp, /*is_leader=*/false);
              return;
          }
          if (config->properties.kvstore == model::kvstore_type::none) {
              return;
          }
          if (is_leader) {
              // Always ensure that if there is a leadership notification
              // emitted, that we also emit a no leader notification, even if
              // the topic is deleted and we no longer have the topic ID.
              topic_id_mapping_.try_emplace(ntp, config->tp_id.value());
          } else {
              topic_id_mapping_.erase(ntp);
          }
          model::topic_id_partition tidp{
            config->tp_id.value(), ntp.tp.partition};
          on_leadership_change(ntp, tidp, is_leader);
      },
      notify_current_state::yes);
    co_return;
}

ss::future<> kvstore_scheduler::stop() {
    if (notification_) {
        auto id = std::exchange(notification_, std::nullopt).value();
        notifier_->unregister_partition_notifications(id);
    }
    co_return;
}

void kvstore_scheduler::on_leadership_change(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  bool is_leader) noexcept {
    ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition;
    if (is_leader) {
        partition = partition_manager_->local().get(ntp);
    }
    if (callback_) {
        callback_(ntp, tidp, partition);
    }
}

} // namespace kvstore
