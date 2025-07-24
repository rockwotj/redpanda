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

#include "cluster/cluster_epoch_service.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "raft/notification.h"
#include "ssx/future-util.h"
#include "ssx/work_queue.h"
#include "utils/backoff_policy.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <fmt/chrono.h>

#include <exception>
#include <stdexcept>

namespace cluster {

namespace {

using namespace std::chrono_literals;

template<typename Clock>
ss::future<int64_t> do_fetch_leader_epoch_impl(
  model::node_id self,
  ss::sharded<partition_leaders_table>* leaders,
  ss::sharded<rpc::connection_cache>* rpc_conn,
  typename Clock::duration timeout) {
    auto cluster_leader = leaders->local().get_leader(model::controller_ntp);
    if (!cluster_leader) {
        throw std::runtime_error(
          "No leader for controller ntp, cannot fetch cluster epoch");
    }
    auto result
      = co_await rpc_conn->local().with_node_client<controller_client_protocol>(
        self,
        ss::this_shard_id(),
        *cluster_leader,
        timeout,
        [timeout](controller_client_protocol client) {
            return client.get_current_cluster_epoch(
              get_current_cluster_epoch_request{.timeout = timeout},
              rpc::client_opts(timeout));
        });
    if (!result) {
        auto ec = result.error();
        throw std::runtime_error(
          fmt::format("Failed to fetch cluster epoch from leader: {}", ec));
    }
    auto resp = result.value().data;
    if (resp.ec != errc::success) {
        throw std::runtime_error(fmt::format(
          "Failed to fetch cluster epoch from leader: {}",
          errc_category().message(static_cast<int>(resp.ec))));
    }
    co_return resp.epoch;
}

} // namespace

// raft0_state is a service that runs on shard0 and is responsible for
// managing the current epoch. The current epoch is a frozen point in time of
// the committed offset of the controller log. We need to use the controller log
// so that we can correlate partition creation with the current epoch.
//
// We periodically "freeze" and refresh the epoch so that we control the epoch
// change and don't have to worry about random cluster operations causing more
// frequent epoch changes (which have the potential for distrupting L0
// operations in cloud topics).
template<typename Clock>
class cluster_epoch_service<Clock>::raft0_state {
public:
    raft0_state(
      ss::lw_shared_ptr<raft::consensus> raft0,
      ss::sharded<controller_stm>& stm,
      ss::sharded<raft::group_manager>& group_manager)
      : _raft0(std::move(raft0))
      , _stm(stm)
      , _group_manager(group_manager)
      , _queue([](const std::exception_ptr& err) {
          vlog(clusterlog.error, "Error in cluster epoch service: {}", err);
      })
      , _leadership_notification_id(
          group_manager.local().register_leadership_notification(
            [this](
              raft::group_id group_id,
              model::term_id,
              std::optional<model::node_id>) noexcept {
                if (group_id != _raft0->group()) {
                    return;
                }
                if (_raft0->is_leader()) {
                    start_service_loop();
                } else {
                    stop_service_loop();
                }
            })) {}

    void start() {
        if (_raft0->is_leader()) {
            start_service_loop();
        } else {
            stop_service_loop();
        }
    }

    // Shutdown this state, stopping the service loop if it's running.
    //
    // Must be called before destruction.
    ss::future<> stop() {
        _group_manager.local().unregister_leadership_notification(
          _leadership_notification_id);
        co_await _queue.shutdown();
        // Now that the queue won't accept new tasks, we can safely stop the
        // service loop if it's running.
        _abort_source.request_abort();
        co_await std::exchange(_loop, std::nullopt)
          .or_else([]() { return std::make_optional(ss::now()); })
          .value()
          .handle_exception([](const std::exception_ptr& ex) {
              vlog(
                clusterlog.error,
                "Error in shutting down cluster epoch service loop: {}",
                ex);
          });
    }

    // Get the current epoch - only set if this node is the leader of raft0.
    std::optional<model::offset> current_epoch() const noexcept {
        return _current_epoch;
    }

private:
    void start_service_loop() noexcept {
        _queue.submit([this] {
            if (_loop) {
                return ss::now();
            }
            _abort_source = {};
            _loop = service_loop();
            return ss::now();
        });
    }
    void stop_service_loop() noexcept {
        _queue.submit([this] {
            _abort_source.request_abort();
            return std::exchange(_loop, std::nullopt)
              .or_else([]() { return std::make_optional(ss::now()); })
              .value();
        });
    }

    ss::future<> service_loop();
    ss::future<> update_epoch();

    ss::lw_shared_ptr<raft::consensus> _raft0;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<raft::group_manager>& _group_manager;
    ssx::work_queue _queue;
    raft::group_manager_notification_id _leadership_notification_id;

    // The following members are used to manage the service loop iff this is
    // shard0 and the current leader of the controller.
    ss::abort_source _abort_source;
    std::optional<ss::future<>> _loop;
    std::optional<model::offset> _current_epoch;
};

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::raft0_state::service_loop() {
    _current_epoch = co_await _stm.local().bootstrap_committed_offset();
    auto cleanup = ss::defer([this]() noexcept { _current_epoch.reset(); });
    while (!_abort_source.abort_requested()) {
        try {
            co_await ss::sleep_abortable<Clock>(
              epoch_bump_interval, _abort_source);
        } catch (const ss::sleep_aborted& e) {
            std::ignore = e;
        }
        if (_abort_source.abort_requested()) {
            co_return;
        }
        co_await update_epoch();
    }
}

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::raft0_state::update_epoch() {
    constexpr std::chrono::milliseconds base_backoff = 100ms;
    constexpr std::chrono::milliseconds max_backoff = 10s;
    constexpr std::chrono::milliseconds rpc_timeout = 3s;
    auto policy = make_exponential_backoff_policy<Clock>(
      base_backoff, max_backoff);
    while (!_abort_source.abort_requested()) {
        try {
            co_await ss::sleep_abortable<Clock>(
              policy.current_backoff_duration(), _abort_source);
            policy.next_backoff(); // if we get a failure increment the backoff
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
        auto deadline = ss::lowres_clock::now() + rpc_timeout;
        auto result_fut
          = co_await ss::coroutine::as_future<result<raft::replicate_result>>(
            _stm.local().quorum_write_empty_batch(deadline));
        if (result_fut.failed()) {
            std::exception_ptr ex = result_fut.get_exception();
            vlog(clusterlog.warn, "Failed to force increment epoch: {}", ex);
            continue;
        }
        auto result = result_fut.get();
        if (result) {
            _current_epoch = result.value().last_offset;
            vlog(
              clusterlog.debug,
              "Forced increment of cluster epoch to: {}",
              _current_epoch);
            co_return;
        } else if (result.error() != raft::errc::not_leader) {
            vlog(
              clusterlog.warn,
              "Failed to force increment epoch: {}",
              result.error());
        }
    }
}

template<typename Clock>
cluster_epoch_service<Clock>::cluster_epoch_service(
  model::node_id self,
  ss::sharded<rpc::connection_cache>* cc,
  ss::sharded<partition_leaders_table>* l) noexcept
  : cluster_epoch_service<Clock>([self, cc, l](Clock::duration timeout) {
      return do_fetch_leader_epoch_impl<Clock>(self, l, cc, timeout);
  }) {}

template<typename Clock>
cluster_epoch_service<Clock>::cluster_epoch_service(
  ss::noncopyable_function<ss::future<int64_t>(typename Clock::duration)>
    fn) noexcept
  : _do_fetch_leader_epoch_fn(std::move(fn)) {}

template<typename Clock>
cluster_epoch_service<Clock>::~cluster_epoch_service() noexcept = default;

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::start() {
    co_return;
}
template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::stop() {
    co_await _gate.close();
    if (!_shard0_state) {
        co_return;
    }
    co_await _shard0_state->stop();
    _shard0_state = nullptr;
}

template<typename Clock>
void cluster_epoch_service<Clock>::invalidate_epoch_cache(
  int64_t epoch_causing_sequence_violation) {
    _gate.check();
    // This is safe to check without the lock, because we only use the lock to
    // limit the number of cross shard calls and RPCs.
    if (_cached_epoch == epoch_causing_sequence_violation) {
        _cached_epoch_time = Clock::time_point::min();
        _epoch_updated_time = Clock::time_point::min();
    }
}

template<typename Clock>
ss::future<int64_t> cluster_epoch_service<Clock>::get_cached_epoch() {
    auto holder = _gate.hold();
    // If the cache entry is needing an update, then block until
    // that update is complete.
    if (cache_entry_needs_updated()) {
        auto units = co_await _mu.get_units();
        if (cache_entry_needs_updated()) {
            co_await update_epoch();
            if (cache_entry_needs_updated()) {
                throw std::runtime_error(fmt::format(
                  "epoch too old, has not successfully updated in {}",
                  std::chrono::duration_cast<std::chrono::seconds>(
                    Clock::now() - _epoch_updated_time)));
            }
        }
    }
    // If the cache entry is expired, then we can spawn an update in the
    // background to update the epoch. Use the lock to ensure we only have
    // a single
    if (cache_entry_expired()) {
        auto maybe_units = _mu.try_get_units();
        if (maybe_units) {
            ssx::spawn_with_gate(
              _gate, [this, units = std::move(*maybe_units)]() mutable {
                  return update_epoch().finally([units = std::move(units)] {});
              });
        }
    }
    co_return _cached_epoch;
}

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::update_epoch() {
    auto maybe_epoch = co_await this->container().invoke_on(
      controller_stm_shard, &cluster_epoch_service::get_current_epoch);
    if (!maybe_epoch) {
        try {
            maybe_epoch = co_await fetch_leader_epoch();
        } catch (...) {
            // fetch_leader_epoch logged, we can just return
            co_return;
        }
    }
    int64_t new_epoch = maybe_epoch.value();
    vassert(
      new_epoch >= _cached_epoch,
      "epochs must monotonically increase, but new epoch {} is less "
      "than the cached epoch of {}",
      new_epoch,
      _cached_epoch);
    _cached_epoch_time = Clock::now();
    if (new_epoch > _cached_epoch) {
        _epoch_updated_time = _cached_epoch_time;
    }
    _cached_epoch = new_epoch;
}

template<typename Clock>
ss::future<int64_t> cluster_epoch_service<Clock>::fetch_leader_epoch() {
    constexpr std::chrono::milliseconds rpc_timeout = 3s;
    constexpr int max_retries = 3;
    std::exception_ptr last_exception;
    int retries = 0;
    while (true) {
        _gate.check();
        try {
            co_return co_await _do_fetch_leader_epoch_fn(rpc_timeout);
        } catch (...) {
            const auto& current = std::current_exception();
            auto done = _gate.is_closed() || ++retries > max_retries;
            vlogl(
              clusterlog,
              done && !ssx::is_shutdown_exception(current)
                ? ss::log_level::warn
                : ss::log_level::debug,
              "Cluster epoch fetch attempt ({} of {}) failed: {}",
              retries,
              max_retries,
              current);
            if (done) {
                throw;
            }
        }
    }
}

template<typename Clock>
ss::future<std::optional<int64_t>>
cluster_epoch_service<Clock>::get_current_epoch() {
    _gate.check();
    if (!_shard0_state) {
        // Normally this should not happen, but to allow testing,
        // we just return std::nullopt here.
        co_return std::nullopt;
    }
    co_return _shard0_state->current_epoch().transform(
      [](auto offset) { return offset(); });
}

template<typename Clock>
void cluster_epoch_service<Clock>::set_raft0(
  ss::lw_shared_ptr<raft::consensus> raft0,
  ss::sharded<controller_stm>& stm,
  ss::sharded<raft::group_manager>& group_manager) noexcept {
    _gate.check();
    vassert(
      ss::this_shard_id() == controller_stm_shard && !_shard0_state,
      "raft0 should only be set on shard0, but was set on {}",
      ss::this_shard_id());
    _shard0_state = std::make_unique<raft0_state>(
      std::move(raft0), stm, group_manager);
    _shard0_state->start();
}

template<typename Clock>
bool cluster_epoch_service<Clock>::cache_entry_expired() const noexcept {
    return Clock::now() > (_cached_epoch_time + epoch_cache_timeout);
}
template<typename Clock>
bool cluster_epoch_service<Clock>::cache_entry_needs_updated() const noexcept {
    return Clock::now() > (_epoch_updated_time + max_same_epoch_cache_duration);
}

template class cluster_epoch_service<ss::lowres_clock>;
template class cluster_epoch_service<ss::manual_clock>;

} // namespace cluster
