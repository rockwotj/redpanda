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

#include "lsm/sst/block_cache.h"

#include "container/chunked_hash_map.h"
#include "ssx/semaphore.h"
#include "utils/s3_fifo.h"

#include <seastar/core/coroutine.hh>

namespace lsm::sst {

struct cache_key {
    cache_key(internal::file_id id, block::handle h)
      : id(id)
      , offset(h.offset) {}

    internal::file_id id;
    uint64_t offset = 0;

    bool operator==(const cache_key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const cache_key& k) {
        return H::combine(std::move(h), k.id, k.offset);
    }
};

class block_cache::impl {
public:
    explicit impl(size_t max_bytes);
    ss::future<> lock(internal::file_id id, block::handle h) {
        auto it = _mu_map.find({id, h});
        ssx::semaphore* mu = nullptr;
        if (it == _mu_map.end()) {
            auto inserted = _mu_map.emplace(
              cache_key{id, h},
              std::make_unique<ssx::semaphore>(
                1, "lsm::sst::block_cache::impl"));
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        co_await mu->wait();
    }
    void unlock(internal::file_id id, block::handle h) noexcept {
        auto it = _mu_map.find({id, h});
        vassert(
          it != _mu_map.end(),
          "unlock must be mirrored with a successful lock call");

        auto& mu = it->second;
        mu->signal();
        // If there are no waiters and no one else now holding the lock, we can
        // cleanup the entry in the map.
        if (mu->waiters() == 0 && mu->available_units() == 1) {
            _mu_map.erase(it);
        }
    }
    void insert(internal::file_id id, block::handle h, block::reader reader) {
        cache_key key{id, h};
        _cache.emplace(
          key, std::make_unique<cached_value>(key, std::move(reader)));
    }
    std::optional<block::reader> get(internal::file_id id, block::handle h) {
        auto it = _cache.find({id, h});
        if (it == _cache.end()) {
            return std::nullopt;
        }
        return it->second->value;
    }

private:
    using ghost_hook_t = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;
    struct cached_value {
        cache_key key;
        block::reader value;
        utils::s3_fifo::cache_hook hook;
        ghost_hook_t ghost_hook;
    };
    using entry_t = std::unique_ptr<cached_value>;
    using ghost_fifo_t = boost::intrusive::list<
      cached_value,
      boost::intrusive::
        member_hook<cached_value, ghost_hook_t, &cached_value::ghost_hook>>;
    struct eviction {
        impl* impl;
        bool operator()(cached_value& e) noexcept {
            impl->_ghost_fifo.push_back(e);
            return true;
        }
    };
    using cache_t = utils::s3_fifo::cache<
      cached_value,
      &cached_value::hook,
      eviction,
      utils::s3_fifo::default_cache_cost>;

    static cache_t::config compute_cache_config(size_t max_entries) {
        // In s3_fifo they recommend the small queue to be ~10% of the main
        // queue. The ghost queue and main queue are the same size. So we split
        // up our queue into 3 chunks:
        // 45% -> main queue
        // 45% -> ghost queue
        // 10% -> small queue
        auto main_cache_size = static_cast<size_t>(
          static_cast<double>(max_entries) * 0.45);
        return cache_t::config{
          .cache_size = main_cache_size,
          .small_size = max_entries - (2 * main_cache_size),
        };
    }

    ghost_fifo_t _ghost_fifo;
    chunked_hash_map<cache_key, std::unique_ptr<ssx::semaphore>> _mu_map;
    chunked_hash_map<cache_key, entry_t> _cache;
};

block_cache::handle::handle(
  block_cache::impl* cache, internal::file_id id, block::handle handle) noexcept
  : _cache(cache)
  , _id(id)
  , _handle(handle) {}

block_cache::handle::~handle() noexcept { _cache->unlock(_id, _handle); }

void block_cache::handle::insert(block::reader rdr) {
    _cache->insert(_id, _handle, std::move(rdr));
}
std::optional<block::reader> block_cache::handle::get() {
    return _cache->get(_id, _handle);
}

block_cache::block_cache(size_t max_bytes)
  : _impl(std::make_unique<impl>(max_bytes)) {}

block_cache::~block_cache() = default;

ss::future<block_cache::handle>
block_cache::get(internal::file_id id, block::handle handle) {
    co_await _impl->lock(id, handle);
    co_return block_cache::handle(_impl.get(), id, handle);
}

} // namespace lsm::sst
