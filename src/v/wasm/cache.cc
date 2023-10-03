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

#include "wasm/cache.h"

#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "ssx/future-util.h"
#include "utils/mutex.h"
#include "wasm/api.h"
#include "wasm/logger.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/later.hh>

#include <absl/container/btree_map.h>

#include <exception>
#include <memory>

namespace wasm {

/**
 * The interval at which we gc factories and engines that are no longer used.
 */
constexpr auto gc_interval = std::chrono::minutes(10);

template<typename Value>
ss::future<>
gc_btree_map(absl::btree_map<model::offset, ss::shared_ptr<Value>>* cache) {
    auto it = cache->begin();
    while (it != cache->end()) {
        // If the cache is the only thing holding a reference to the entry,
        // then we can delete the factory from the cache.
        if (it->second.use_count() == 1) {
            it = cache->erase(it);
        }

        if (ss::need_preempt()) {
            // The iterator could have be invalidated if there was a write
            // during the yield. We'll use the ordered nature of the btree to
            // support resuming the iterator after the suspension point.
            model::offset checkpoint = it->first;
            co_await ss::yield();
            it = cache->lower_bound(checkpoint);
        }
    }
}

struct cached_factory {
    std::unique_ptr<factory> factory;
};

namespace {

/**
 * Allows sharing an engine between multiple uses.
 *
 * Must live on a single core.
 */
class shared_engine : public engine {
public:
    explicit shared_engine(std::unique_ptr<engine> underlying)
      : _underlying(std::move(underlying)) {}

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        auto u = co_await _mu.get_units();
        auto fut = co_await ss::coroutine::as_future<model::record_batch>(
          _underlying->transform(std::move(batch), probe));
        if (fut.available()) {
            co_return fut.get();
        }
        // Restart the engine
        try {
            co_await _underlying->stop();
            co_await _underlying->start();
        } catch (...) {
            vlog(
              wasm_log.warn,
              "failed to restart wasm engine: {}",
              std::current_exception());
        }
        std::rethrow_exception(fut.get_exception());
    }

    ss::future<> start() override {
        auto u = co_await _mu.get_units();
        if (++_ref_count == 1) {
            co_await _underlying->start();
        }
    }
    ss::future<> stop() override {
        auto u = co_await _mu.get_units();
        if (--_ref_count == 0) {
            co_await _underlying->stop();
        }
    }

    std::string_view function_name() const override {
        return _underlying->function_name();
    }
    uint64_t memory_usage_size_bytes() const override {
        return _underlying->memory_usage_size_bytes();
    }

private:
    mutex _mu;
    size_t _ref_count = 0;
    std::unique_ptr<engine> _underlying;
};

/**
 * Allows moving a shared_ptr engine into a unique_ptr
 */
class wrapped_engine : public engine {
public:
    explicit wrapped_engine(ss::shared_ptr<engine> underlying)
      : _underlying(std::move(underlying)) {}

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        return _underlying->transform(std::move(batch), probe);
    }

    ss::future<> start() override { return _underlying->start(); }
    ss::future<> stop() override { return _underlying->stop(); }

    std::string_view function_name() const override {
        return _underlying->function_name();
    }
    uint64_t memory_usage_size_bytes() const override {
        return _underlying->memory_usage_size_bytes();
    }

private:
    ss::shared_ptr<engine> _underlying;
};

} // namespace

/** A cache for engines on a particular core. */
class engine_cache {
public:
    void put(model::offset offset, std::unique_ptr<engine> engine) {
        _cache.emplace(
          offset, ss::make_shared<shared_engine>(std::move(engine)));
    }

    std::unique_ptr<engine> get(model::offset offset) {
        auto it = _cache.find(offset);
        if (it == _cache.end()) {
            return nullptr;
        }
        return std::make_unique<wrapped_engine>(it->second);
    }

    ss::future<> gc() { return gc_btree_map(&_cache); }

private:
    absl::btree_map<model::offset, ss::shared_ptr<engine>> _cache;
};

namespace {

class wrapped_factory : public factory {
public:
    explicit wrapped_factory(
      ss::foreign_ptr<ss::shared_ptr<cached_factory>> f,
      model::offset offset,
      ss::sharded<engine_cache>* e)
      : _offset(offset)
      , _underlying(std::move(f))
      , _engine_cache(e) {}

    ss::future<std::unique_ptr<engine>> make_engine() override {
        auto engine = _engine_cache->local().get(_offset);
        // Try to grab an engine outside the lock
        if (engine) {
            co_return engine;
        }
        // Acquire the lock
        auto u = co_await _mu.get_units();
        // Double check nobody created one while we were grabbing the lock.
        engine = _engine_cache->local().get(_offset);
        if (engine) {
            co_return engine;
        }
        // Create the actual engine and put it in the cache.
        auto created = co_await _underlying->factory->make_engine();
        _engine_cache->local().put(_offset, std::move(created));
        co_return _engine_cache->local().get(_offset);
    }

private:
    mutex _mu;
    model::offset _offset;
    ss::foreign_ptr<ss::shared_ptr<cached_factory>> _underlying;
    ss::sharded<engine_cache>* _engine_cache;
};

} // namespace

caching_runtime::caching_runtime(std::unique_ptr<runtime> u)
  : _underlying(std::move(u))
  , _gc_timer(
      [this]() { ssx::spawn_with_gate(_gate, [this] { return do_gc(); }); }) {}

caching_runtime::~caching_runtime() = default;

ss::future<> caching_runtime::start() {
    co_await _underlying->start();
    co_await _engine_caches.start();
    _gc_timer.arm(gc_interval);
}

ss::future<> caching_runtime::stop() {
    _gc_timer.cancel();
    co_await _gate.close();
    co_await _engine_caches.stop();
    co_await _underlying->stop();
}

/**
 * A RAII scoped lock that ensures factory locks are deleted when there are no
 * waiters.
 */
class factory_mutex_lock_guard {
public:
    factory_mutex_lock_guard(const factory_mutex_lock_guard&) = delete;
    factory_mutex_lock_guard& operator=(const factory_mutex_lock_guard&)
      = delete;
    factory_mutex_lock_guard(factory_mutex_lock_guard&&) noexcept = default;
    factory_mutex_lock_guard& operator=(factory_mutex_lock_guard&&) noexcept
      = default;

    static ss::future<factory_mutex_lock_guard> acquire(
      absl::btree_map<model::offset, mutex>* mu_map, model::offset offset) {
        auto it = mu_map->find(offset);
        mutex* mu = nullptr;
        if (it == mu_map->end()) {
            auto inserted = mu_map->emplace(offset, mutex{});
            vassert(inserted.second, "expected mutex to be inserted");
            mu = &inserted.first->second;
        } else {
            mu = &it->second;
        }
        mutex::units units = co_await mu->get_units();
        co_return factory_mutex_lock_guard(
          offset, mu_map, mu, std::move(units));
    }

    ~factory_mutex_lock_guard() {
        _underlying.return_all();
        if (_mu->ready()) {
            _mu_map->erase(_offset);
        }
    }

private:
    factory_mutex_lock_guard(
      model::offset offset,
      absl::btree_map<model::offset, mutex>* mu_map,
      mutex* mu,
      mutex::units underlying)
      : _offset(offset)
      , _mu_map(mu_map)
      , _mu(mu)
      , _underlying(std::move(underlying)) {}

    model::offset _offset;
    absl::btree_map<model::offset, mutex>* _mu_map;
    mutex* _mu;
    mutex::units _underlying;
};

ss::future<std::unique_ptr<factory>> caching_runtime::make_factory(
  model::transform_metadata meta, iobuf binary, ss::logger* logger) {
    auto h = _gate.hold();
    model::offset offset = meta.source_ptr;
    // Look in the cache outside the lock
    auto it = _factory_cache.find(offset);
    if (it != _factory_cache.end()) {
        co_return std::make_unique<wrapped_factory>(
          ss::make_foreign(it->second), offset, &_engine_caches);
    }
    auto lg = co_await factory_mutex_lock_guard::acquire(&_mu_map, offset);
    // Look again in the cache with the lock
    it = _factory_cache.find(offset);
    if (it != _factory_cache.end()) {
        co_return std::make_unique<wrapped_factory>(
          ss::make_foreign(it->second), offset, &_engine_caches);
    }
    // There is no factory and we're holding the lock,
    // time to create a new one.
    auto factory = co_await _underlying->make_factory(
      std::move(meta), std::move(binary), logger);

    // Now cache the factory and return the result.
    auto cached = ss::make_shared<cached_factory>(std::move(factory));
    auto [_, inserted] = _factory_cache.emplace(offset, cached);
    vassert(inserted, "expected factory to be inserted");

    co_return std::make_unique<wrapped_factory>(
      ss::make_foreign(cached), offset, &_engine_caches);
}

ss::future<> caching_runtime::do_gc() {
    auto fut = co_await ss::coroutine::as_future(
      ss::when_all(gc_factories(), gc_engines()));
    if (fut.failed()) {
        vlog(
          wasm_log.warn,
          "wasm caching runtime gc failed: {}",
          std::move(fut).get_exception());
    }
    _gc_timer.arm(gc_interval);
}

ss::future<> caching_runtime::gc_factories() {
    return gc_btree_map(&_factory_cache);
}

ss::future<> caching_runtime::gc_engines() {
    return _engine_caches.invoke_on_all(&engine_cache::gc);
}
} // namespace wasm
