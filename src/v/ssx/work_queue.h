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

#pragma once

#include "seastarx.h"
#include "ssx/future-util.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/stream.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <exception>
#include <type_traits>
#include <utility>

namespace ssx {

/**
 * A small utility for running async tasks sequentially.
 */
class work_queue {
public:
    using error_reporter_fn
      = ss::noncopyable_function<void(const std::exception_ptr&)>;

    explicit work_queue(error_reporter_fn);
    // Add a task to the queue to be processed.
    void submit(ss::noncopyable_function<ss::future<>()>);
    // Add a task to the queue to be processed.
    template<typename Clock>
    void submit_delayed(
      typename Clock::duration, ss::noncopyable_function<ss::future<>()>);
    // Shutdown the queue, waiting for the currently executing task to finish.
    ss::future<> shutdown();

private:
    error_reporter_fn _error_reporter;
    ss::future<> _tail = ss::now();
    ss::abort_source _as;
    uint64_t _latest_id{0};
    absl::flat_hash_map<uint64_t, ss::future<>> _pending;
};

template<typename Clock>
void work_queue::submit_delayed(
  typename Clock::duration duration,
  ss::noncopyable_function<ss::future<>()> task) {
    auto my_id = ++_latest_id;
    auto pending_op = ss::sleep_abortable<Clock>(duration, _as)
                        .then_wrapped([this, my_id, task = std::move(task)](
                                        ss::future<> fut) mutable {
                            fut.ignore_ready_future();
                            _pending.erase(my_id);
                            // submit will noop if aborted
                            submit(std::move(task));
                        });
    _pending.emplace(my_id, std::move(pending_op));
}

/**
 * A utility for running sequential work on a ss::thread
 *
 * Conceptionally calling `enqueue` is the same as ss::async, except that it is
 * much faster. ss::async creates a ss::thread object which allocates a new
 * stack and swaps out the stack using some ucontext APIs, so using this queue
 * we can minimalize the cost of setting up a ss::thread.
 */
class threaded_work_queue {
public:
    ss::future<> start();
    ss::future<> stop();

    template<typename T>
    ss::future<T> enqueue(ss::noncopyable_function<T()> fn) {
        ss::promise<T> p;
        try {
            co_await submit([&p, fn = std::move(fn)] {
                try {
                    if constexpr (std::is_void_v<T>) {
                        fn();
                        p.set_value();
                    } else {
                        p.set_value(fn());
                    }
                } catch (...) {
                    p.set_to_current_exception();
                }
            });
        } catch (...) {
            p.set_to_current_exception();
        }
        co_return co_await std::move(p.get_future());
    }

private:
    ss::future<> submit(ss::noncopyable_function<void()>);
    mutex _submit_mutex;
    ss::promise<ss::noncopyable_function<void()>> _pending_task;
    ss::promise<> _pending_task_completed;
    ss::abort_source _as;
    ss::thread _thread{};
};

} // namespace ssx
