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

#include "lsm/db/table_cache.h"

#include "container/chunked_hash_map.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/sst/reader.h"
#include "utils/mutex.h"
#include "utils/s3_fifo.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/weak_ptr.hh>

namespace lsm::db {

class table_cache::impl : public ss::weakly_referencable<impl> {
    class wrapped_iterator : public internal::iterator {
    public:
        wrapped_iterator(
          ss::weak_ptr<table_cache::impl> cache,
          internal::file_id file_id,
          std::unique_ptr<internal::iterator> underlying)
          : _cache(std::move(cache))
          , _file_id(file_id)
          , _underlying(std::move(underlying)) {}
        wrapped_iterator(const wrapped_iterator&) = delete;
        wrapped_iterator(wrapped_iterator&&) = delete;
        wrapped_iterator& operator=(const wrapped_iterator&) = delete;
        wrapped_iterator& operator=(wrapped_iterator&&) = delete;
        ~wrapped_iterator() override {
            if (_cache) {
                _cache->queue_cleanup_if_evicted(_file_id);
            }
        }
        bool valid() const override { return _underlying->valid(); }
        ss::future<> seek_to_first() override {
            return _underlying->seek_to_first();
        }
        ss::future<> seek_to_last() override {
            return _underlying->seek_to_last();
        }
        ss::future<> seek(internal::key_view target) override {
            return _underlying->seek(target);
        }
        ss::future<> next() override { return _underlying->next(); }
        ss::future<> prev() override { return _underlying->prev(); }
        internal::key_view key() override { return _underlying->key(); }
        iobuf value() override { return _underlying->value(); }

    private:
        ss::weak_ptr<table_cache::impl> _cache;
        internal::file_id _file_id;
        std::unique_ptr<internal::iterator> _underlying;
    };

public:
    impl(io::persistence* p, int32_t max_entries)
      : _persistence(p) {
        std::ignore = max_entries;
    }

    ss::future<std::unique_ptr<internal::iterator>>
    create_iterator(internal::file_id id, uint64_t file_size) {
        auto table = co_await find_reader(id, file_size);
        co_return std::make_unique<wrapped_iterator>(
          weak_from_this(), id, table->create_iterator());
    }

    ss::future<> get(
      internal::file_id id,
      uint64_t file_size,
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
        auto table = co_await find_reader(id, file_size);
        co_await table->internal_get(key, fn);
    }

    ss::future<> evict(internal::file_id id) {
        co_await gc_ghost_fifo();
        auto it = _map.find(id);
        if (it == _map.end()) {
            co_return;
        }
        _cache.remove(*it->second);
        _ghost_fifo.erase(_ghost_fifo.iterator_to(*it->second));
        co_await it->second->value->close();
        _map.erase(it);
    }

    void queue_cleanup_if_evicted(internal::file_id id) {
        // TODO: What to do??
        std::ignore = id;
    }

private:
    using ghost_hook_t = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    struct cached_value {
        internal::file_id id;
        ss::lw_shared_ptr<sst::reader> value;
        utils::s3_fifo::cache_hook hook;
        ghost_hook_t ghost_hook;
    };

    using entry_t = std::unique_ptr<cached_value>;
    using ghost_fifo_t = boost::intrusive::list<
      cached_value,
      boost::intrusive::
        member_hook<cached_value, ghost_hook_t, &cached_value::ghost_hook>>;

    struct eviction {
        table_cache::impl* impl;
        bool operator()(cached_value& e) noexcept {
            e.value = nullptr;
            impl->_ghost_fifo.push_back(e);
            return true;
        }
    };

    using cache_t = utils::s3_fifo::cache<
      cached_value,
      &cached_value::hook,
      eviction,
      utils::s3_fifo::default_cache_cost>;

    ss::future<ss::lw_shared_ptr<sst::reader>>
    find_reader(internal::file_id id, uint64_t file_size) {
        co_await gc_ghost_fifo();
        auto it = _map.find(id);
        if (it == _map.end()) {
            auto units = co_await _mu.get_units();
            auto reader = co_await open_reader(id, file_size);
            auto [it, succ] = _map.try_emplace(
              id, std::make_unique<cached_value>(id, std::move(reader)));
            if (!succ) {
                co_return it->second->value;
            }
            _cache.insert(*it->second);
            co_return it->second->value;
        }
        auto& entry = *it->second;
        if (entry.hook.evicted()) {
            entry.value = co_await open_reader(id, file_size);
            _ghost_fifo.erase(_ghost_fifo.iterator_to(entry));
            _cache.insert(entry);
        }
        entry.hook.touch();
        co_return entry.value;
    }

    ss::future<ss::lw_shared_ptr<sst::reader>>
    open_reader(internal::file_id id, uint64_t file_size) {
        auto file = co_await _persistence->open_random_access_reader(
          internal::sst_file_name(id));
        if (!file) {
            throw invalid_argument_exception("file for ID {} is not found", id);
        }
        auto reader = co_await sst::reader::open(std::move(*file), file_size);
        co_return ss::make_lw_shared(std::move(reader));
    }

    ss::future<> gc_ghost_fifo() {
        for (auto it = _ghost_fifo.begin(); it != _ghost_fifo.end();) {
            auto& entry = *it;
            if (_cache.ghost_queue_contains(entry)) {
                // The ghost queue is in fifo-order so any entry that comes
                // after an entry that hasn't been evicted will also not be
                // evicted.
                co_return;
            }
            // TODO: figure this out!
            if (entry.value && entry.value.use_count() == 1) {
                co_await entry.value->close();
            }
            it = _ghost_fifo.erase(it);
            _map.erase(entry.id);
        }
    }

    mutex _mu;
    io::persistence* _persistence;
    chunked_hash_map<internal::file_id, entry_t> _map;
    cache_t _cache;
    ghost_fifo_t _ghost_fifo;
};

table_cache::table_cache(io::persistence* persistence, int32_t max_entries)
  : _impl(std::make_unique<impl>(persistence, max_entries)) {}

table_cache::~table_cache() = default;

ss::future<std::unique_ptr<internal::iterator>>
table_cache::create_iterator(internal::file_id id, uint64_t file_size) {
    return _impl->create_iterator(id, file_size);
}

ss::future<> table_cache::get(
  internal::file_id id,
  uint64_t file_size,
  internal::key_view key,
  absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
    return _impl->get(id, file_size, key, fn);
}

ss::future<> table_cache::evict(internal::file_id id) {
    return _impl->evict(id);
}

} // namespace lsm::db
