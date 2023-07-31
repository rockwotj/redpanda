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
#include "transform/transform_processor.h"
#include "vassert.h"
#include "vlog.h"
#include "wasm/api.h"
#include "wasm/errc.h"
#include "wasm/probe.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index_container.hpp>

#include <algorithm>
#include <chrono>
#include <future>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

namespace transform {

// The underlying container for holding processors and indexing them correctly.
//
// It's possible for there to be the same ntp with multiple transforms and it's
// also possible for a transform to be attached to multiple ntps, so the true
// "key" for a processor is the tuple of (id, ntp), hence the need for the
// complicated table of indexes.
class processor_table {
    struct key_index_t {};
    struct ntp_index_t {};
    struct id_index_t {};

    using underlying_t = boost::multi_index::multi_index_container<
      // a set of processors
      std::unique_ptr<processor>,
      boost::multi_index::indexed_by<
        // uniquely indexed by id and ntp together
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<key_index_t>,
          boost::multi_index::composite_key<
            std::unique_ptr<processor>,
            boost::multi_index::
              const_mem_fun<processor, cluster::transform_id, &processor::id>,
            boost::multi_index::
              const_mem_fun<processor, const model::ntp&, &processor::ntp>>,
          boost::multi_index::composite_key_hash<
            std::hash<cluster::transform_id>,
            std::hash<model::ntp>>>,
        // indexed by ntp
        boost::multi_index::hashed_non_unique<
          boost::multi_index::tag<ntp_index_t>,
          boost::multi_index::
            const_mem_fun<processor, const model::ntp&, &processor::ntp>,
          std::hash<model::ntp>,
          std::equal_to<>>,
        // indexed by id
        boost::multi_index::hashed_non_unique<
          boost::multi_index::tag<id_index_t>,
          boost::multi_index::
            const_mem_fun<processor, cluster::transform_id, &processor::id>,
          std::hash<cluster::transform_id>,
          std::equal_to<>>>>;

public:
    processor* insert(std::unique_ptr<processor> p) {
        auto [it, inserted] = _underlying.emplace(std::move(p));
        vassert(inserted, "invalid transform::stm management");
        return it->get();
    }

    bool contains(cluster::transform_id id, const model::ntp& ntp) {
        auto& by_key = _underlying.get<key_index_t>();
        return by_key.contains(std::make_tuple(id, ntp));
    }

    size_t count_by_id(cluster::transform_id id) {
        auto& by_id = _underlying.get<id_index_t>();
        return by_id.count(id);
    }

    ss::future<> clear() {
        std::vector<ss::future<>> futures;
        futures.reserve(_underlying.size());
        for (auto& entry : _underlying) {
            futures.push_back(entry->stop());
        }
        co_await ss::when_all(futures.begin(), futures.end());
        _underlying.clear();
    }

    ss::future<> erase_by_id(cluster::transform_id id) {
        auto& by_id = _underlying.get<id_index_t>();
        auto range = by_id.equal_range(id);
        co_await ss::parallel_for_each(
          range.first,
          range.second,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& p) { return p->stop(); });
        by_id.erase(range.first, range.second);
    }

    // Clear our all the transforms with a given ntp and return all the IDs that
    // no longer exist.
    ss::future<absl::flat_hash_set<cluster::transform_id>>
    erase_by_ntp(model::ntp ntp) {
        auto& by_ntp = _underlying.get<ntp_index_t>();
        auto range = by_ntp.equal_range(ntp);
        co_await ss::parallel_for_each(
          range.first,
          range.second,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& p) { return p->stop(); });
        absl::flat_hash_set<cluster::transform_id> ids;
        for (auto it = range.first; it != range.second; ++it) {
            ids.insert(it->get()->id());
        }
        by_ntp.erase(range.first, range.second);
        auto& by_id = _underlying.get<id_index_t>();
        // Erase everything that is still around, we only want to return the
        // ones that were deleted.
        absl::erase_if(ids, [&by_id](auto id) { return by_id.contains(id); });
        co_return ids;
    }

private:
    underlying_t _underlying;
};

manager::manager(
  wasm::runtime* rt,
  std::unique_ptr<plugin_registry> pr,
  std::unique_ptr<source::factory> srcf,
  std::unique_ptr<sink::factory> outf)
  : _runtime(rt)
  , _registry(std::move(pr))
  , _processors(std::make_unique<processor_table>())
  , _queue([](const std::exception_ptr& ex) {
      vlog(tlog.error, "unexpected transform manager error: {}", ex);
  })
  , _source_factory(std::move(srcf))
  , _sink_factory(std::move(outf)) {}

manager::~manager() = default;

ss::future<> manager::start() { return ss::now(); }
ss::future<> manager::stop() {
    vlog(tlog.info, "Stopping transform manager...");
    co_await _queue.shutdown();
    co_await _processors->clear();
    for (auto& entry : _probes) {
        entry.second->clear_metrics();
    }
    _probes.clear();
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
  cluster::transform_id id,
  model::partition_id partition_id,
  cluster::transform_metadata meta) {
    _queue.submit([this, id, partition_id, meta = std::move(meta)]() mutable {
        return handle_plugin_error(id, partition_id, std::move(meta));
    });
}

ss::future<>
manager::handle_leadership_change(model::ntp ntp, ntp_leader leader_status) {
    vlog(
      tlog.debug,
      "handling leadership status change to leader={} for: {}",
      leader_status,
      ntp);

    if (leader_status == ntp_leader::no) {
        // We're not the leader anymore, time to shutdown all the transforms
        auto ids = co_await _processors->erase_by_ntp(ntp);
        for (const auto& id : ids) {
            erase_probe(id);
        }
        co_return;
    }
    // We are leader - start all the stms that aren't already running
    auto transforms = _registry->lookup_by_input_topic(
      model::topic_namespace_view(ntp));
    co_await ss::parallel_for_each(
      std::move(transforms), [this, &ntp](auto entry) {
          if (_processors->contains(entry.first, ntp)) {
              return ss::now();
          }
          return start_processor(
            ntp, entry.first, std::move(entry.second), /*attempts=*/0);
      });
}

ss::future<> manager::handle_plugin_change(cluster::transform_id id) {
    vlog(tlog.debug, "handling update to plugin: {}", id);
    // If we have an existing stm we need to restart it with the updates
    // applied.
    co_await _processors->erase_by_id(id);
    erase_probe(id);

    auto transform = _registry->lookup_by_id(id);
    // If there is no transform we're good to go, everything is shutdown if
    // needed.
    if (!transform) {
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
          start_processor(std::move(ntp), id, *transform, /*attempts=*/0));
    }
    co_await ss::when_all(futures.begin(), futures.end());
}

ss::future<> manager::handle_plugin_error(
  cluster::transform_id id,
  model::partition_id partition_id,
  cluster::transform_metadata meta) {
    vlog(
      tlog.debug, "handling plugin error {} on partition {}", id, partition_id);
    _registry->report_error(id, partition_id, std::move(meta));
    co_await _processors->erase_by_id(id);
    erase_probe(id);
}

void manager::erase_probe(cluster::transform_id id) {
    auto it = _probes.find(id);
    if (it == _probes.end()) {
        return;
    }
    it->second->clear_metrics();
    _probes.erase(it);
}

void manager::attempt_start_processor(
  model::ntp ntp, cluster::transform_id id, size_t attempts) {
    // TODO: Do a delay before attempting to start again.
    _queue.submit([this, ntp = std::move(ntp), id, attempts]() mutable {
        return do_attempt_start_processor(std::move(ntp), id, attempts);
    });
}

ss::future<> manager::do_attempt_start_processor(
  model::ntp ntp, cluster::transform_id id, size_t attempts) {
    // It's possible something else came along and kicked this stm and that
    // worked.
    if (_processors->contains(id, ntp)) {
        co_return;
    }
    auto transform = _registry->lookup_by_id(id);
    // This transform was deleted
    if (!transform || transform->failed_partitions.contains(ntp.tp.partition)) {
        co_return;
    }
    constexpr size_t max_attempts = 3;
    if (++attempts > max_attempts) {
        vlog(
          tlog.warn,
          "failed to create transform {} for {}, marking as failed",
          transform->name,
          ntp);
        on_plugin_error(id, ntp.tp.partition, *transform);
        co_return;
    }
    auto leaders = _registry->get_leader_partitions(
      model::topic_namespace_view(ntp));
    // no longer a leader for this partition
    if (!leaders.contains(ntp.tp.partition)) {
        co_return;
    }
    co_await start_processor(ntp, id, *transform, attempts);
}
wasm::transform_probe* manager::get_or_create_probe(
  cluster::transform_id id, const cluster::transform_name& name) {
    auto it = _probes.find(id);
    if (it != _probes.end()) {
        return it->second.get();
    }
    auto [pit, inserted] = _probes.emplace(
      id, std::make_unique<wasm::transform_probe>());
    vassert(inserted, "double insert into probes map");
    pit->second->setup_metrics(
      name(), [this, id] { return _processors->count_by_id(id); });
    return pit->second.get();
}

ss::future<> manager::start_processor(
  model::ntp ntp,
  cluster::transform_id id,
  cluster::transform_metadata meta,
  size_t attempts) {
    if (meta.failed_partitions.contains(ntp.tp.partition)) {
        vlog(
          tlog.info,
          "not starting transform {} on failed partition {}",
          meta.name,
          ntp);
        co_return;
    }
    std::unique_ptr<processor> created;
    try {
        constexpr std::chrono::milliseconds fetch_binary_timeout
          = std::chrono::seconds(2);
        auto binary = co_await _registry->fetch_binary(
          meta.source_ptr, fetch_binary_timeout);
        if (!binary) {
            vlog(
              tlog.warn,
              "missing source binary for {} offset={}, marking as failed",
              meta.name,
              meta.source_ptr);
            // enqueue a task to mark the plugin as failed
            on_plugin_error(id, ntp.tp.partition, meta);
            co_return;
        }
        // TODO: we should be sharing factories across the entire process, maybe
        // we need some sort of cache?
        auto factory = co_await _runtime->make_factory(
          meta, std::move(binary).value(), &tlog);
        auto engine = co_await factory->make_engine();
        auto src = _source_factory->create(ntp);
        if (!src) {
            vlog(
              tlog.warn,
              "unable to create transform {} input source to {}, retrying...",
              meta.name,
              ntp);
            attempt_start_processor(ntp, id, attempts);
            co_return;
        }
        std::vector<std::unique_ptr<sink>> sinks;
        sinks.reserve(meta.output_topics.size());
        for (const auto& output_topic : meta.output_topics) {
            auto output_ntp = model::ntp(
              output_topic.ns, output_topic.tp, ntp.tp.partition);
            auto sink = _sink_factory->create(output_ntp);
            if (!sink) {
                vlog(
                  tlog.warn,
                  "unable to create transform {} output sink to {}, "
                  "retrying...",
                  meta.name,
                  output_ntp);
                attempt_start_processor(ntp, id, attempts);
                co_return;
            }
            sinks.emplace_back(std::move(sink).value());
        }
        auto* probe = get_or_create_probe(id, meta.name);
        created = std::make_unique<processor>(
          id,
          ntp,
          meta,
          std::move(engine),
          [this](
            cluster::transform_id id,
            model::partition_id partition_id,
            cluster::transform_metadata meta) {
              on_plugin_error(id, partition_id, std::move(meta));
          },
          std::move(src).value(),
          std::move(sinks),
          probe);
    } catch (const wasm::wasm_exception& ex) {
        vlog(
          tlog.warn,
          "invalid wasm source unable to create transform::stm: {}, marking as "
          "failed",
          ex);
        // enqueue a task to mark the plugin as failed
        on_plugin_error(id, ntp.tp.partition, meta);
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "unable to create transform::stm: {}, retrying...", ex);
        // requeue a task to start the stm
        attempt_start_processor(ntp, id, attempts);
    }
    if (!created) {
        co_return;
    }
    // Ensure that we insert this transform into our mapping before we start it.
    auto* p = _processors->insert(std::move(created));
    try {
        co_await p->start();
    } catch (const std::exception& ex) {
        vlog(
          tlog.warn,
          "invalid wasm source unable to start transform::stm: {}, marking as "
          "failed",
          ex);
        on_plugin_error(id, ntp.tp.partition, meta);
    }
    vlog(tlog.info, "started transform {} on {}", id, ntp);
}

} // namespace transform
