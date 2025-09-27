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

#include "lsm/db/impl.h"

#include "lsm/core/internal/merging_iterator.h"
#include "lsm/sst/block_cache.h"

#include <seastar/core/coroutine.hh>

#include <memory>

namespace lsm::db {

impl::impl(
  ctor,
  std::unique_ptr<io::persistence> p,
  ss::lw_shared_ptr<internal::options> o)
  : _persistence(std::move(p))
  , _opts(std::move(o))
  , _mem(ss::make_lw_shared<memtable>())
  , _table_cache(
      std::make_unique<table_cache>(
        _persistence.get(),
        _opts->max_open_files,
        ss::make_lw_shared<sst::block_cache>(_opts->block_cache_size)))
  , _versions(
      std::make_unique<version_set>(
        _persistence.get(), _table_cache.get(), _opts)) {}

ss::future<std::unique_ptr<impl>> impl::open(
  ss::lw_shared_ptr<internal::options> opts,
  std::unique_ptr<io::persistence> persistence) {
    auto db = std::make_unique<impl>(
      ctor{}, std::move(persistence), std::move(opts));
    co_await db->recover();
    co_return db;
}

ss::future<> impl::put(internal::key_view key, iobuf value) {
    _mem->add(internal::key(key), std::move(value));
    co_return;
}

ss::future<> impl::remove(internal::key_view key) {
    _mem->remove(internal::key(key));
    co_return;
}

ss::future<std::optional<iobuf>> impl::get(internal::key_view key) {
    version::get_stats stats{};
    co_return co_await _versions->current()->get(key, &stats);
}

ss::future<std::unique_ptr<internal::iterator>> impl::create_iterator() {
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.push_back(_mem->create_iterator());
    if (_imm) {
        list.push_back((*_imm)->create_iterator());
    }
    co_await _versions->current()->add_iterators(&list);
    co_return internal::create_merging_iterator(std::move(list));
}

ss::future<> impl::close() {
    co_await _table_cache->close();
    co_await _persistence->close();
}

ss::future<> impl::recover() {
    co_await _versions->recover();
    co_return;
}

} // namespace lsm::db
