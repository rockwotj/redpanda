// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/work_queue.h"

#include "vassert.h"

#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>

#include <exception>
#include <optional>
#include <utility>
#include <vector>

namespace ssx {
work_queue::work_queue(error_reporter_fn fn)
  : _error_reporter(std::move(fn)) {}
void work_queue::submit(ss::noncopyable_function<ss::future<>()> fn) {
    if (_as.abort_requested()) {
        return;
    }
    _tail = _tail
              .then([this, fn = std::move(fn)]() {
                  if (_as.abort_requested()) {
                      return ss::now();
                  }
                  return fn();
              })
              .handle_exception(
                [this](const std::exception_ptr& ex) { _error_reporter(ex); });
}
ss::future<> work_queue::shutdown() {
    _as.request_abort();
    std::vector<ss::future<>> pending;
    for (auto& [_, f] : _pending) {
        pending.push_back(std::move(f));
    }
    co_await ss::when_all_succeed(pending.begin(), pending.end());
    vassert(
      _pending.empty(),
      "expected all delayed tasks to be finished, {} left",
      _pending.size());
    co_await std::exchange(_tail, ss::now());
}

ss::future<> threaded_work_queue::submit(ss::noncopyable_function<void()> fn) {
    auto _ = co_await _submit_mutex.get_units();
    _as.check();
    _pending_task_completed = {};
    _pending_task.set_value(std::move(fn));
    co_await _pending_task_completed.get_future();
}

ss::future<> threaded_work_queue::start() {
    _pending_task = {};
    _pending_task_completed = {};
    _as = {};
    _thread = ss::thread([this] {
        while (!_as.abort_requested()) {
            auto task = _pending_task.get_future().get();
            _pending_task = {};
            try {
                task();
                _pending_task_completed.set_value();
            } catch (...) {
                _pending_task_completed.set_to_current_exception();
            }
        }
    });
    return ss::now();
}
ss::future<> threaded_work_queue::stop() {
    // already have stopped
    if (_as.abort_requested()) {
        co_return;
    }
    co_await submit([this] { _as.request_abort(); });
    co_await _thread.join();
}
} // namespace ssx
