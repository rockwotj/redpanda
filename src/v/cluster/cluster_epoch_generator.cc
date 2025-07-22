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

#include "cluster/cluster_epoch_generator.h"

#include "base/vassert.h"
#include "cluster/controller_stm.h"
#include "cluster/logger.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

namespace cluster {

namespace {

using namespace std::chrono_literals;

// TODO(cloud-topics): make these configuration knobs
constexpr ss::lowres_clock::duration epoch_cache_timeout = 1s;
// The maximum amount of time to tolerate the same epoch being returned
constexpr ss::lowres_clock::duration max_same_epoch_duration = 1min;

// NOLINTNEXTLINE(*-avoid-reference-coroutine-parameters)
ss::future<> force_increment_loop(controller_stm& stm, ss::abort_source& as) {
    model::offset last_offset = co_await stm.bootstrap_committed_offset();
    while (!as.abort_requested()) {
        try {
            co_await ss::sleep_abortable<ss::lowres_clock>(
              max_same_epoch_duration, as);
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
        model::offset new_offset = co_await stm.bootstrap_committed_offset();
        if (new_offset > last_offset) {
            last_offset = new_offset;
            continue;
        }
        auto result = co_await stm.quorum_write_empty_batch(
          ss::lowres_clock::now() + 3s);
        if (result) {
            last_offset = result.value().last_offset;
            vlog(
              clusterlog.debug,
              "Forced increment of cluster epoch to: {}",
              last_offset);
        } else if (result.error() != raft::errc::not_leader) {
            vlog(
              clusterlog.warn,
              "Failed to force increment epoch: {}",
              result.error());
        }
    }
}

} // namespace

struct cluster_epoch_generator::shard0_state {
    ss::sharded<controller_stm>& stm;
    ss::future<> force_increment_loop;
    ss::abort_source abort_source;
};

cluster_epoch_generator::cluster_epoch_generator() noexcept = default;
cluster_epoch_generator::~cluster_epoch_generator() noexcept = default;

ss::future<> cluster_epoch_generator::start() { co_return; }
ss::future<> cluster_epoch_generator::stop() {
    co_await _gate.close();
    if (_shard0_state) {
        _shard0_state->abort_source.request_abort();
        co_await std::move(_shard0_state->force_increment_loop);
        _shard0_state = nullptr;
    }
}

ss::future<int64_t> cluster_epoch_generator::current_epoch() {
    auto holder = _gate.hold();
    if (cache_entry_expired()) {
        auto units = co_await _mu.get_units();
        if (cache_entry_expired()) {
            int64_t new_epoch = co_await this->container().invoke_on(
              controller_stm_shard,
              &cluster_epoch_generator::get_current_epoch);
            vassert(
              new_epoch >= _cached_epoch,
              "epochs must monotonically increase, but new epoch {} is less "
              "than the cached epoch of {}",
              new_epoch,
              _cached_epoch);
            _cached_epoch = new_epoch;
            _cached_epoch_time = ss::lowres_clock::now();
        }
    }
    co_return _cached_epoch;
}

ss::future<int64_t> cluster_epoch_generator::get_current_epoch() {
    vassert(
      _shard0_state,
      "get_current_epoch called on cluster_epoch_generator without "
      "controller_stm");
    auto co = co_await _shard0_state->stm.local().bootstrap_committed_offset();
    co_return co();
}

void cluster_epoch_generator::set_raft0(
  ss::sharded<controller_stm>& stm) noexcept {
    _gate.check();
    vassert(
      ss::this_shard_id() == controller_stm_shard,
      "raft0 should only be set on shard0, but was set on {}",
      ss::this_shard_id());
    _shard0_state = std::make_unique<shard0_state>(stm, ss::now());
    _shard0_state->force_increment_loop = force_increment_loop(
      stm.local(), _shard0_state->abort_source);
}

bool cluster_epoch_generator::cache_entry_expired() const noexcept {
    return ss::lowres_clock::now() > (_cached_epoch_time + epoch_cache_timeout);
}
} // namespace cluster
