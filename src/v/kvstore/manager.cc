/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kvstore/manager.h"

#include "cloud_storage_clients/types.h"
#include "cluster/partition.h"
#include "config/node_config.h"
#include "kvstore/db.h"
#include "kvstore/logger.h"
#include "model/fundamental.h"

namespace kvstore {

namespace {

std::filesystem::path local_path(model::topic_id_partition tidp) {
    return config::node().kvstore_path()
           / fmt::format("{}/{}", tidp.topic_id(), tidp.partition());
}

cloud_storage_clients::object_key remote_path(model::topic_id_partition tidp) {
    // TODO: Do we want to prefix with cluster ID?
    return cloud_storage_clients::object_key{std::filesystem::path{
      fmt::format("kvstores/{}/{}", tidp.topic_id(), tidp.partition())}};
}

} // namespace

kvstore_manager::kvstore_manager(
  cloud_io::remote* r, cloud_storage_clients::bucket_name b)
  : _remote(r)
  , _bucket(std::move(b))
  , _queue([](const std::exception_ptr& ex) {
      vlog(kvlog.error, "error in kvstore manager: {}", ex);
  }) {}

ss::future<> kvstore_manager::start() { co_return; }
ss::future<> kvstore_manager::stop() {
    co_await _queue.shutdown();
    for (auto& db : _dbs) {
        co_await db.second->stop();
    }
}

void kvstore_manager::schedule_partition(
  model::ntp ntp,
  model::topic_id_partition tidp,
  ss::lw_shared_ptr<cluster::partition> p) {
    _queue.submit([this, ntp = std::move(ntp), tidp, p = std::move(p)] mutable {
        return do_schedule_partition(std::move(ntp), tidp, std::move(p));
    });
}

void kvstore_manager::unschedule_partition(
  model::ntp ntp, model::topic_id_partition tidp) {
    _queue.submit([this, ntp = std::move(ntp), tidp] mutable {
        return do_unschedule_partition(std::move(ntp), tidp);
    });
}

ss::future<> kvstore_manager::do_schedule_partition(
  model::ntp ntp,
  model::topic_id_partition tidp,
  ss::lw_shared_ptr<cluster::partition> partition) {
    auto it = _dbs.find(ntp);
    if (it != _dbs.end()) {
        co_return;
    }
    auto database = db::make(
      std::move(partition),
      _remote,
      _bucket,
      remote_path(tidp),
      local_path(tidp));
    co_await database->start();
    _dbs.emplace(std::move(ntp), std::move(database));
}

ss::future<> kvstore_manager::do_unschedule_partition(
  model::ntp ntp, model::topic_id_partition tidp) {
    std::ignore = tidp; // Right now we key off ntp
    auto it = _dbs.find(ntp);
    if (it == _dbs.end()) {
        co_return;
    }
    auto [_, db] = _dbs.extract(it);
    co_await db->stop();
}

} // namespace kvstore
