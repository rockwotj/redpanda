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

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "rpc/backoff_policy.h"
#include "ssx/future-util.h"
#include "transform/logger.h"
#include "transform/transform_processor.h"
#include "utils/human.h"
#include "vassert.h"
#include "vlog.h"
#include "wasm/api.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index_container.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

namespace transform {

using namespace std::chrono_literals;

namespace {
template<typename ClockType>
class processor_backoff {
public:
    static constexpr std::chrono::seconds base_duration = 1s;
    static constexpr std::chrono::seconds max_duration = 30s;
    // If a processor has been running for this long we mark it as good.
    static constexpr std::chrono::seconds reset_duration = 10s;

    // Mark that we attempted to start a processor, this is used to ensure that
    // we properly reset backoff if the processor has been running for long
    // enough.
    void mark_start_attempt() { _start_time = ClockType::now(); }

    ClockType::duration next_backoff_duration() {
        auto now = ClockType::now();
        if (now > (_start_time + reset_duration)) {
            _backoff.reset();
        }
        _backoff.next_backoff();
        return _backoff.current_backoff_duration();
    }

private:
    // the last time we started this processor
    ClockType::time_point _start_time = ClockType::now();
    // the backoff policy for this processor for when we attempt to restart
    // the processor. If it's been enough time since our last restart of the
    // processor we will reset this.
    ::rpc::backoff_policy _backoff
      = ::rpc::make_exponential_backoff_policy<ClockType>(
        base_duration, max_duration);
};

} // namespace

// The underlying container for holding processors and indexing them correctly.
//
// It's possible for there to be the same ntp with multiple transforms and it's
// also possible for a transform to be attached to multiple ntps, so the true
// "key" for a processor is the tuple of (id, ntp), hence the need for the
// complicated table of indexes.
template<typename ClockType>
class processor_table {
    struct key_index_t {};
    struct ntp_index_t {};
    struct id_index_t {};

    class entry_t {
    public:
        entry_t(
          std::unique_ptr<transform::processor> processor,
          ss::lw_shared_ptr<transform::probe> probe)
          : _processor(std::move(processor))
          , _probe(std::move(probe))
          , _backoff(std::make_unique<processor_backoff<ClockType>>()) {}

        processor* processor() const { return _processor.get(); }
        ss::lw_shared_ptr<probe> probe() const { return _probe; }
        processor_backoff<ClockType>* backoff() const { return _backoff.get(); }

        const model::ntp& ntp() const { return _processor->ntp(); }
        model::transform_id id() const { return _processor->id(); }

    private:
        std::unique_ptr<transform::processor> _processor;
        ss::lw_shared_ptr<transform::probe> _probe;
        std::unique_ptr<processor_backoff<ClockType>> _backoff;
    };

    using underlying_t = boost::multi_index::multi_index_container<
      entry_t,
      boost::multi_index::indexed_by<
        // uniquely indexed by id and ntp together
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<key_index_t>,
          boost::multi_index::composite_key<
            entry_t,
            boost::multi_index::
              const_mem_fun<entry_t, model::transform_id, &entry_t::id>,
            boost::multi_index::
              const_mem_fun<entry_t, const model::ntp&, &entry_t::ntp>>,
          boost::multi_index::composite_key_hash<
            std::hash<model::transform_id>,
            std::hash<model::ntp>>>,
        // indexed by ntp
        boost::multi_index::hashed_non_unique<
          boost::multi_index::tag<ntp_index_t>,
          boost::multi_index::
            const_mem_fun<entry_t, const model::ntp&, &entry_t::ntp>,
          std::hash<model::ntp>,
          std::equal_to<>>,
        // indexed by id
        boost::multi_index::hashed_non_unique<
          boost::multi_index::tag<id_index_t>,
          boost::multi_index::
            const_mem_fun<entry_t, model::transform_id, &entry_t::id>,
          std::hash<model::transform_id>,
          std::equal_to<>>>>;

public:
    const entry_t& insert(
      std::unique_ptr<processor> processor,
      ss::lw_shared_ptr<transform::probe> probe) {
        auto [it, inserted] = _underlying.emplace(
          std::move(processor), std::move(probe));
        vassert(inserted, "invalid transform processor management");
        return *it;
    }

    ss::lw_shared_ptr<probe> get_or_create_probe(
      model::transform_id id, const model::transform_metadata& meta) {
        auto& id_index = _underlying.template get<id_index_t>();
        auto it = id_index.find(id);
        if (it == id_index.end()) {
            auto probe = ss::make_lw_shared<transform::probe>();
            probe->setup_metrics(meta.name);
            return probe;
        }
        return it->probe();
    }

    bool contains(model::transform_id id, const model::ntp& ntp) {
        auto& by_key = _underlying.template get<key_index_t>();
        return by_key.contains(std::make_tuple(id, ntp));
    }

    const entry_t* get_or_null(model::transform_id id, const model::ntp& ntp) {
        auto& by_key = _underlying.template get<key_index_t>();
        auto it = by_key.find(std::make_tuple(id, ntp));
        if (it == by_key.end()) {
            return nullptr;
        }
        return std::addressof(*it);
    }

    ss::future<> clear() {
        co_await ss::parallel_for_each(
          _underlying.begin(),
          _underlying.end(),
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& e) { return e.processor()->stop(); });
        _underlying.clear();
    }

    ss::future<> erase_by_id(model::transform_id id) {
        auto& by_id = _underlying.template get<id_index_t>();
        auto range = by_id.equal_range(id);
        co_await ss::parallel_for_each(
          range.first,
          range.second,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& e) { return e.processor()->stop(); });
        by_id.erase(range.first, range.second);
    }

    // Clear our all the transforms with a given ntp and return all the IDs that
    // no longer exist.
    ss::future<> erase_by_ntp(model::ntp ntp) {
        auto& by_ntp = _underlying.template get<ntp_index_t>();
        auto range = by_ntp.equal_range(ntp);
        co_await ss::parallel_for_each(
          range.first,
          range.second,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& e) { return e.processor()->stop(); });
        by_ntp.erase(range.first, range.second);
    }

private:
    underlying_t _underlying;
};

template<typename ClockType>
manager<ClockType>::manager(
  std::unique_ptr<registry> r, std::unique_ptr<processor_factory> f)
  : _queue([](const std::exception_ptr& ex) {
      vlog(tlog.error, "unexpected transform manager error: {}", ex);
  })
  , _registry(std::move(r))
  , _processors(std::make_unique<processor_table<ClockType>>())
  , _processor_factory(std::move(f)) {}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    return ss::now();
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    vlog(tlog.info, "Stopping transform manager...");
    co_await _queue.shutdown();
    vlog(tlog.info, "Stopped queue");
    co_await _processors->clear();
    vlog(tlog.info, "Stopped transform manager.");
}

template<typename ClockType>
void manager<ClockType>::on_leadership_change(
  model::ntp ntp, ntp_leader leader_status) {
    _queue.submit([this, ntp = std::move(ntp), leader_status]() mutable {
        return handle_leadership_change(std::move(ntp), leader_status);
    });
}

template<typename ClockType>
void manager<ClockType>::on_plugin_change(model::transform_id id) {
    _queue.submit([this, id] { return handle_plugin_change(id); });
}

template<typename ClockType>
void manager<ClockType>::on_transform_error(
  model::transform_id id, model::ntp ntp, model::transform_metadata meta) {
    _queue.submit(
      [this, id, ntp = std::move(ntp), meta = std::move(meta)]() mutable {
          return handle_transform_error(id, std::move(ntp), std::move(meta));
      });
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_leadership_change(
  model::ntp ntp, ntp_leader leader_status) {
    vlog(
      tlog.debug,
      "handling leadership status change to leader={} for: {}",
      leader_status,
      ntp);

    if (leader_status == ntp_leader::no) {
        // We're not the leader anymore, time to shutdown all the processors
        co_await _processors->erase_by_ntp(ntp);
        co_return;
    }
    // We're the leader - start all the processor that aren't already running
    auto transforms = _registry->lookup_by_input_topic(
      model::topic_namespace_view(ntp));
    co_await ss::parallel_for_each(
      transforms, [this, ntp = std::move(ntp)](model::transform_id id) {
          return start_processor(ntp, id);
      });
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_plugin_change(model::transform_id id) {
    vlog(tlog.debug, "handling update to plugin: {}", id);
    // If we have an existing processor we need to restart it with the updates
    // applied.
    co_await _processors->erase_by_id(id);

    auto transform = _registry->lookup_by_id(id);
    // If there is no transform we're good to go, everything is shutdown if
    // needed.
    if (!transform) {
        co_return;
    }
    // Otherwise, start a processor for every partition we're a leader of.
    auto partitions = _registry->get_leader_partitions(transform->input_topic);
    auto probe = _processors->get_or_create_probe(id, *transform);
    co_await ss::parallel_for_each(
      partitions,
      [this, id, transform = *std::move(transform), p = std::move(probe)](
        model::partition_id partition_id) {
          auto ntp = model::ntp(
            transform.input_topic.ns, transform.input_topic.tp, partition_id);
          // It's safe to directly create processors, because we deleted them
          // for a full restart from this deploy
          return create_processor(std::move(ntp), id, transform, p);
      });
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_transform_error(
  model::transform_id id, model::ntp ntp, model::transform_metadata meta) {
    auto* entry = _processors->get_or_null(id, ntp);
    if (!entry) {
        co_return;
    }
    co_await entry->processor()->stop();
    auto delay = entry->backoff()->next_backoff_duration();
    vlog(
      tlog.info,
      "transform {} errored on partition {}, delaying for {} then restarting",
      meta.name,
      ntp.tp.partition,
      human::latency(delay));
    _queue.submit_delayed<ClockType>(delay, [this, id, ntp]() mutable {
        return start_processor(std::move(ntp), id);
    });
}

template<typename ClockType>
ss::future<>
manager<ClockType>::start_processor(model::ntp ntp, model::transform_id id) {
    auto* entry = _processors->get_or_null(id, ntp);
    // It's possible something else came along and kicked this processor and
    // that worked.
    if (entry && entry->processor()->is_running()) {
        co_return;
    }
    auto transform = _registry->lookup_by_id(id);
    // This transform was deleted, if there is an entry, it *should* have a
    // pending delete notification enqueued (if there is an existing entry).
    if (!transform) {
        co_return;
    }
    auto leaders = _registry->get_leader_partitions(
      model::topic_namespace_view(ntp));
    // no longer a leader for this partition, if there was an entry, it
    // *should* have a pending no_leader notification enqueued (that will
    // cleanup the existing entry).
    if (!leaders.contains(ntp.tp.partition)) {
        co_return;
    }
    if (entry) {
        entry->backoff()->mark_start_attempt();
        co_await entry->processor()->start();
    } else {
        auto probe = _processors->get_or_create_probe(id, *transform);
        co_await create_processor(
          ntp, id, *std::move(transform), std::move(probe));
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::create_processor(
  model::ntp ntp,
  model::transform_id id,
  model::transform_metadata meta,
  ss::lw_shared_ptr<probe> p) {
    auto fut = co_await ss::coroutine::as_future(
      _processor_factory->create_processor(
        id,
        ntp,
        meta,
        [this](auto id, model::ntp ntp, model::transform_metadata meta) {
            on_transform_error(id, std::move(ntp), std::move(meta));
        },
        p.get()));
    if (fut.failed()) {
        vlog(
          tlog.warn,
          "failed to create transform processor {}: {}, retrying...",
          meta.name,
          fut.get_exception());
        // Delay some time before attempting to recreate a processor
        // TODO: Should we have more sophisticated backoff mechanisms?
        constexpr auto recreate_attempt_delay = 30s;
        _queue.submit_delayed<ClockType>(
          recreate_attempt_delay, [this, ntp = std::move(ntp), id]() mutable {
              return start_processor(std::move(ntp), id);
          });
    } else {
        // Ensure that we insert this transform into our mapping before we
        // start it, so that if start fails and calls the error callback
        // we properly know that it's in flight.
        auto& entry = _processors->insert(std::move(fut).get(), std::move(p));
        vlog(tlog.info, "starting transform {} on {}", meta.name, ntp);
        entry.backoff()->mark_start_attempt();
        co_await entry.processor()->start();
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::drain_queue_for_test() {
    ss::promise<> p;
    auto f = p.get_future();
    // Move the promise into the queue so we get
    // broken promise exceptions if the queue is shutdown.
    _queue.submit([p = std::move(p)]() mutable {
        p.set_value();
        return ss::now();
    });
    co_await std::move(f);
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform
