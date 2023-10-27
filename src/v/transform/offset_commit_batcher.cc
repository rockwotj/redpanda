#include "transform/offset_commit_batcher.h"

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/transform.h"
#include "rpc/backoff_policy.h"
#include "ssx/future-util.h"
#include "transform/logger.h"
#include "vlog.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <exception>

namespace transform {

template<typename ClockType>
offset_commit_batcher<ClockType>::offset_commit_batcher(
  ClockType::duration commit_interval, std::unique_ptr<offset_committer> oc)
  : _offset_committer(std::move(oc))
  , _commit_interval(commit_interval)
  , _timer(
      [this] { ssx::spawn_with_gate(_gate, [this] { return flush(); }); }) {}

template<typename ClockType>
ss::future<> offset_commit_batcher<ClockType>::start() {
    ssx::spawn_with_gate(_gate, [this] { return find_coordinator_loop(); });
    return ss::now();
}

template<typename ClockType>
ss::future<> offset_commit_batcher<ClockType>::find_coordinator_loop() {
    using namespace std::chrono;
    constexpr static auto base_backoff = 100ms;
    constexpr static auto max_backoff = 5000ms;
    auto backoff = ::rpc::make_exponential_backoff_policy<ClockType>(
      base_backoff, max_backoff);
    while (!_gate.is_closed()) {
        co_await _unbatched_cond_var.wait(
          [this] { return !_unbatched.empty() || _gate.is_closed(); });
        bool was_all_failures = co_await assign_coordinators();
        if (!was_all_failures) {
            backoff.reset();
            continue;
        }
        backoff.next_backoff();
        try {
            co_await ss::sleep_abortable<ClockType>(
              backoff.current_backoff_duration(), _as);
        } catch (const ss::sleep_aborted&) {
            // do nothing, we're shutting down.
        }
    }
}

template<typename ClockType>
ss::future<bool> offset_commit_batcher<ClockType>::assign_coordinators() {
    bool was_all_failures = !_unbatched.empty();
    auto it = _unbatched.begin();
    while (it != _unbatched.end() && !_gate.is_closed()) {
        model::transform_offsets_key key = it->first;
        auto fut = co_await ss::coroutine::as_future<
          result<model::partition_id, cluster::errc>>(
          _offset_committer->find_coordinator(key));
        if (fut.failed()) {
            vlog(
              tlog.warn,
              "unable to determine key ({}) coordinator: {}",
              key,
              fut.get_exception());
            it = _unbatched.upper_bound(key);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              tlog.warn,
              "unable to determine key ({}) coordinator: {}",
              key,
              cluster::error_category().message(int(result.error())));
            it = _unbatched.upper_bound(key);
            continue;
        }
        was_all_failures = false;
        model::partition_id coordinator = result.value();
        auto entry = _unbatched.extract(key);
        // It's possible that the value was removed while the request was being
        // made, in that case just skip the update.
        if (!entry) {
            continue;
        }
        _batched[coordinator].insert_or_assign(entry.key(), entry.mapped());
        _coordinator_cache[key] = coordinator;
        if (!_timer.armed()) {
            _timer.arm(_commit_interval);
        }
    }
    co_return was_all_failures;
}

template<typename ClockType>
ss::future<> offset_commit_batcher<ClockType>::stop() {
    _timer.cancel();
    _as.request_abort();
    _unbatched_cond_var.signal();
    co_await _gate.close();
    co_await flush();
}

template<typename ClockType>
ss::future<>
offset_commit_batcher<ClockType>::preload(model::transform_offsets_key) {
    // For now, don't do anything, we will resolve it first time they key needs
    // to commit a batch. In the future we can preload this, mostly to surface
    // any errors (like too many keys, etc) before reading and processing any
    // batches.
    return ss::now();
}

template<typename ClockType>
ss::future<>
offset_commit_batcher<ClockType>::unload(model::transform_offsets_key key) {
    // Erase the key from the coordinator cache so that we don't
    _coordinator_cache.erase(key);
    _unbatched.erase(key);
    // We don't need to clear batched, we can allow it to be flushed async.
    return ss::now();
}

template<typename ClockType>
ss::future<> offset_commit_batcher<ClockType>::commit_offset(
  model::transform_offsets_key k, model::transform_offsets_value v) {
    auto _ = _gate.hold();
    auto it = _coordinator_cache.find(k);
    if (it == _coordinator_cache.end()) {
        _unbatched.insert_or_assign(k, v);
        _unbatched_cond_var.signal();
    } else {
        _batched[it->second].insert_or_assign(k, v);
        if (!_timer.armed()) {
            _timer.arm(_commit_interval);
        }
    }
    return ss::now();
}

template<typename ClockType>
ss::future<> offset_commit_batcher<ClockType>::flush() {
    auto it = _batched.begin();
    while (it != _batched.end()) {
        auto node = _batched.extract(it);
        model::partition_id coordinator = node.key();
        kv_map kvs = std::move(node.mapped());
        auto fut = co_await ss::coroutine::as_future<>(
          _offset_committer->batch_commit(
            node.key(), std::move(node.mapped())));
        if (fut.failed()) {
            vlog(
              tlog.warn,
              "unable to commit batch (coordinator={}): {}",
              coordinator,
              fut.get_exception());
            it = _batched.upper_bound(coordinator);
            continue;
        }
    }
    if (!_batched.empty() && !_timer.armed()) {
        _timer.arm(_commit_interval);
    }
}

template class offset_commit_batcher<ss::lowres_clock>;
template class offset_commit_batcher<ss::manual_clock>;
} // namespace transform
