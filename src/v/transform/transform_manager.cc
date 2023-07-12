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
#include "transform/transform_manager.h"

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/future-util.h"
#include "transform/logger.h"
#include "transform/transform_stm.h"
#include "vlog.h"
#include "wasm/api.h"
#include "wasm/errc.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <algorithm>
#include <chrono>
#include <future>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

namespace transform {

manager::manager(
  wasm::runtime* rt,
  std::unique_ptr<plugin_registry> pr,
  std::unique_ptr<source::factory> srcf,
  std::unique_ptr<sink::factory> outf)
  : _runtime(rt)
  , _registry(std::move(pr))
  , _source_factory(std::move(srcf))
  , _sink_factory(std::move(outf)) {}

manager::~manager() = default;

ss::future<> manager::start() { return ss::now(); }
ss::future<> manager::stop() {
    vlog(tlog.info, "Stopping transform manager...");
    co_await _queue.shutdown();
    // Now shutdown all the stms
    std::vector<ss::future<>> futures;
    futures.reserve(_stms_by_ntp.size());
    for (auto& entry : _stms_by_ntp) {
        futures.push_back(entry.second->stop());
    }
    co_await ss::when_all(futures.begin(), futures.end());
    _stms_by_id.clear();
    _stms_by_ntp.clear();
    vlog(tlog.info, "Stopped transform manager.");
}

void manager::on_leadership_change(model::ntp ntp, ntp_leader leader_status) {
    _queue.submit([this, ntp = std::move(ntp), leader_status]() mutable {
        return handle_leadership_change(std::move(ntp), leader_status);
    });
}
void manager::on_plugin_change(cluster::transform_id id) {
    _queue.submit([this, id] { return handle_plugin_change(id); });
}
void manager::on_plugin_error(
  cluster::transform_id id, cluster::transform_metadata meta) {
    _queue.submit([this, id, meta = std::move(meta)]() mutable {
        return handle_plugin_error(id, std::move(meta));
    });
}

ss::future<>
manager::handle_leadership_change(model::ntp ntp, ntp_leader leader_status) {
    vlog(
      tlog.debug,
      "handling leadership status change to leader={} for: {}",
      leader_status,
      ntp);
    auto it = _stms_by_ntp.find(ntp);
    if (it == _stms_by_ntp.end()) {
        // We aren't the leader and we don't have an stm running, good to
        // go.
        if (leader_status == ntp_leader::no) {
            co_return;
        }
        // We need to start an stm
        auto transforms = _registry->lookup_by_input_topic(
          model::topic_namespace_view(ntp));
        co_await ss::parallel_for_each(
          std::move(transforms), [this, ntp](auto entry) {
              return start_stm(
                ntp, entry.first, std::move(entry.second), /*attempts=*/0);
          });
        co_return;
    }
    // There is nothing to do, we're already running and we're managing as
    // the leader.
    if (leader_status == ntp_leader::yes) {
        co_return;
    }
    // Time to stop our stm
    co_await it->second->stop();
    vassert(_stms_by_id.erase(it->second->id()) > 0, "index inconsistency");
    _stms_by_ntp.erase(it);
}

ss::future<> manager::handle_plugin_change(cluster::transform_id id) {
    vlog(tlog.debug, "handling update to plugin: {}", id);
    auto transform = _registry->lookup_by_id(id);
    auto it = _stms_by_id.find(id);
    // If we have an existing stm we need to restart it with the updates
    // applied.
    if (it != _stms_by_id.end()) {
        co_await it->second->stop();
        vassert(
          _stms_by_ntp.erase(it->second->ntp()) > 0, "index inconsistency");
        _stms_by_id.erase(it);
    }
    // If there is no transform we're good to go, everything is shutdown if
    // needed.
    if (!transform || transform->paused) {
        co_return;
    }
    // Otherwise, start a stm for every partition we're a leader of.
    auto partitions = _registry->get_leader_partitions(transform->input_topic);
    std::vector<ss::future<>> futures;
    futures.reserve(partitions.size());
    for (auto partition : partitions) {
        auto ntp = model::ntp(
          transform->input_topic.ns, transform->input_topic.tp, partition);
        futures.push_back(
          start_stm(std::move(ntp), id, *transform, /*attempts=*/0));
    }
    co_await ss::when_all(futures.begin(), futures.end());
}

ss::future<> manager::handle_plugin_error(
  cluster::transform_id id, cluster::transform_metadata meta) {
    vlog(tlog.debug, "handling plugin error: {}", id);
    _registry->report_error(id, meta);
    auto it = _stms_by_id.find(id);
    if (it == _stms_by_id.end()) {
        co_return;
    }
    co_await it->second->stop();
    vassert(_stms_by_ntp.erase(it->second->ntp()) > 0, "index inconsistency");
    _stms_by_id.erase(it);
}

void manager::attempt_start_stm(
  model::ntp ntp, cluster::transform_id id, size_t attempts) {
    // TODO: Do a delay before attempting to start again.
    _queue.submit([this, ntp = std::move(ntp), id, attempts]() mutable {
        return do_attempt_start_stm(std::move(ntp), id, attempts);
    });
}
ss::future<> manager::do_attempt_start_stm(
  model::ntp ntp, cluster::transform_id id, size_t attempts) {
    // It's possible something else came along and kicked this stm and that
    // worked.
    if (_stms_by_id.contains(id)) {
        co_return;
    }
    auto transform = _registry->lookup_by_id(id);
    // This transform was deleted
    if (!transform) {
        co_return;
    }
    constexpr size_t max_attempts = 3;
    if (++attempts > max_attempts) {
        vlog(
          tlog.warn,
          "failed to create transform {}, marking as failed",
          transform->name);
        on_plugin_error(id, *transform);
        co_return;
    }
    auto leaders = _registry->get_leader_partitions(
      model::topic_namespace_view(ntp));
    // no longer a leader for this partition
    if (!leaders.contains(ntp.tp.partition)) {
        co_return;
    }
    co_await start_stm(ntp, id, *transform, attempts);
}

ss::future<> manager::start_stm(
  model::ntp ntp,
  cluster::transform_id id,
  cluster::transform_metadata meta,
  size_t attempts) {
    std::unique_ptr<stm> s;
    try {
        constexpr std::chrono::milliseconds fetch_binary_timeout
          = std::chrono::seconds(2);
        auto binary = co_await _registry->fetch_binary(
          meta.source_ptr, fetch_binary_timeout);
        if (!binary) {
            vlog(
              tlog.warn,
              "missing source binary, offset={}, marking as failed",
              meta.source_ptr);
            // enqueue a task to mark the plugin as failed
            on_plugin_error(id, meta);
            co_return;
        }
        // TODO: we should be sharing factories across the entire process, maybe
        // we need some sort of cache?
        auto factory = co_await _runtime->make_factory(
          meta, std::move(binary).value());
        auto engine = co_await factory->make_engine();
        auto src = _source_factory->create(ntp);
        if (!src) {
            vlog(
              tlog.warn,
              "unable to create transform::stm input source, retrying...");
            attempt_start_stm(ntp, id, attempts);
            co_return;
        }
        std::vector<std::unique_ptr<sink>> sinks;
        sinks.reserve(meta.output_topics.size());
        for (const auto& output_topic : meta.output_topics) {
            auto sink = _sink_factory->create(
              model::ntp(output_topic.ns, output_topic.tp, ntp.tp.partition));
            if (!sink) {
                vlog(
                  tlog.warn,
                  "unable to create transform::stm output sink, retrying...");
                attempt_start_stm(ntp, id, attempts);
                co_return;
            }
            sinks.emplace_back(std::move(sink).value());
        }
        s = std::make_unique<stm>(
          id,
          ntp,
          std::move(meta),
          std::move(engine),
          [this](cluster::transform_id id, cluster::transform_metadata meta) {
              on_plugin_error(id, std::move(meta));
          },
          std::move(src).value(),
          std::move(sinks));
    } catch (const wasm::wasm_exception& ex) {
        vlog(
          tlog.warn,
          "invalid wasm source unable to create transform::stm: {}, marking as "
          "failed",
          ex);
        // enqueue a task to mark the plugin as failed
        on_plugin_error(id, meta);
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "unable to create transform::stm: {}, retrying...", ex);
        // requeue a task to start the stm
        attempt_start_stm(ntp, id, attempts);
    }
    if (!s) {
        co_return;
    }
    // Ensure that we insert this transform into our mapping before we start it.
    _stms_by_id[id] = s.get();
    auto [it, inserted] = _stms_by_ntp.emplace(ntp, std::move(s));
    vassert(
      inserted, "invalid transform::stm management for id={} ntp={}", id, ntp);
    co_await it->second->start();
    vlog(tlog.info, "started transform {} on {}", id, ntp);
}

} // namespace transform
