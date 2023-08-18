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
#include "utils/mutex.h"
#include "vassert.h"
#include "wasm/logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <sys/eventfd.h>

#include <csignal>
#include <exception>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <type_traits>

namespace wasm {

namespace impl {

class task_base {
public:
    task_base() = default;
    task_base(task_base&&) = delete;
    task_base(task_base const&) = delete;
    task_base& operator=(task_base&&) = delete;
    task_base& operator=(task_base const&) = delete;

    virtual void process(ss::alien::instance&, ss::shard_id) = 0;
    virtual void
    set_exception(ss::alien::instance&, ss::shard_id, std::exception_ptr)
      = 0;
    virtual ~task_base() = default;
};

template<typename Func>
class worker_task final : public task_base {
    using value_type = std::invoke_result_t<Func>;

public:
    explicit worker_task(Func func)
      : _func{std::move(func)} {}

    ss::future<value_type> get_future() noexcept {
        return _promise.get_future();
    }

    void process(ss::alien::instance& alien, ss::shard_id shard) final {
        try {
            if constexpr (std::is_void_v<value_type>) {
                _func();
                ss::alien::run_on(
                  alien, shard, [this]() noexcept { _promise.set_value(); });
            } else {
                auto v = _func();
                ss::alien::run_on(
                  alien, shard, [this, v{std::move(v)}]() mutable noexcept {
                      _promise.set_value(std::move(v));
                  });
            }
        } catch (...) {
            set_exception(alien, shard, std::current_exception());
        };
    }

    void set_exception(
      ss::alien::instance& alien,
      ss::shard_id shard,
      std::exception_ptr p) final {
        ss::alien::run_on(
          alien, shard, [this, p]() noexcept { _promise.set_exception(p); });
    }

private:
    Func _func;
    typename ss::promise<value_type> _promise;
};

class thread_worker {
    static constexpr eventfd_t RUNNABLE_INIT = 0;
    static constexpr eventfd_t RUNNABLE_READY = 1;
    static constexpr eventfd_t RUNNABLE_STOP = 2;

public:
    thread_worker()
      : _alien{std::addressof(ss::engine().alien())}
      , _cpu_id(::sched_getcpu())
      , _shard_id(ss::this_shard_id()) {}

    void start() {
        _worker = std::thread([this]() { return run(); });
    }

    ss::future<> stop() {
        co_await _gate.close();
        _ready.signal(RUNNABLE_STOP);
        if (_worker.joinable()) {
            _worker.join();
        }
    }

    template<typename Func>
    auto submit(Func func) {
        auto gh = _gate.hold();
        return _mutex.get_units().then(
          [this, gh = std::move(gh), func = std::move(func)](
            auto units) mutable {
              vassert(_task == nullptr, "Only one task supported at a time");
              auto task = std::make_unique<impl::worker_task<Func>>(
                std::move(func));
              auto f = task->get_future().finally(
                [this, gh{std::move(gh)}, units{std::move(units)}] {
                    _task.reset();
                });
              _task = std::move(task);
              _ready.signal(RUNNABLE_READY);
              return f;
          });
    }

private:
    void configure_thread() {
        ::pthread_setname_np(::pthread_self(), "wasm::thread_worker");
        auto all_signals = ss::make_full_sigset_mask();
        ss::throw_pthread_error(
          ::pthread_sigmask(SIG_UNBLOCK, &all_signals, nullptr));
        // pin to core
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(_cpu_id, &cs);
        auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
        vassert(r == 0, "Can not pin executor thread to core {}", _cpu_id);
        wasm_log.info("started wasmtime thread", _shard_id);
    }
    size_t run() {
        configure_thread();
        while (true) {
            eventfd_t result = 0;
            auto r = ::eventfd_read(_ready.get_read_fd(), &result);
            if (r < 0 || result != RUNNABLE_READY) {
                if (_task) {
                    _task->set_exception(
                      *_alien,
                      _shard_id,
                      std::make_exception_ptr(ss::abort_requested_exception{}));
                }
            }
            if (r < 0) {
                return r;
            } else if (result == RUNNABLE_STOP) {
                return 0;
            }
            _task->process(*_alien, _shard_id);
        }
    }

    ss::alien::instance* _alien;
    unsigned int _cpu_id;
    ss::shard_id _shard_id;
    std::thread _worker;
    ss::gate _gate;
    mutex _mutex;
    seastar::writeable_eventfd _ready{RUNNABLE_INIT};
    std::unique_ptr<impl::task_base> _task{nullptr};
};

} // namespace impl

/**
 * thread_worker runs tasks in a std::thread.
 *
 * By running in a std::thread, it's possible to make blocking calls such as
 * file I/O and posix thread primitives without blocking a reactor.
 *
 * The thread worker will drain all operations before joining the thread in
 * stop(), but it should be noted that joining a thread may block. As such, this
 * class is most suited to run for the lifetime of an application, rather than
 * short-lived.
 */
class thread_worker {
public:
    thread_worker() = default;

    /**
     * start the background thread.
     */
    ss::future<> start() {
        _impl.start();
        return ss::make_ready_future<>();
    }

    /**
     * stop and join the background thread.
     *
     * Although the work has completed, it should be noted that joining a thread
     * may block the reactor.
     */
    ss::future<> stop() {
        co_await _gate.close();
        co_await _impl.stop();
    }

    /**
     * submit a task to the background thread
     */
    template<typename Func>
    auto submit(Func func) ->
      typename ss::futurize<std::invoke_result_t<Func>>::type {
        return ss::with_gate(_gate, [this, func{std::move(func)}]() mutable {
            return _impl.submit(std::move(func));
        });
    }

private:
    ss::gate _gate;
    impl::thread_worker _impl;
};

} // namespace wasm
