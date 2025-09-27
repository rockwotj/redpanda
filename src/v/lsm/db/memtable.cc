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

#include "lsm/db/memtable.h"

#include "absl/container/btree_map.h"
#include "base/vassert.h"

#include <seastar/util/variant_utils.hh>

#include <memory>
#include <variant>

namespace lsm::db {

class memtable::iterator : public internal::iterator {
public:
    // The dummy iterator is only a place holder for the linked list in the
    // memtable.
    struct dummy {};
    explicit iterator(dummy) {}

    explicit iterator(ss::lw_shared_ptr<memtable> memtable)
      : _mem(std::move(memtable))
      , _it(_mem->_table.end()) {}
    iterator(const iterator&) = delete;
    iterator(iterator&&) = delete;
    iterator& operator=(const iterator&) = delete;
    iterator& operator=(iterator&&) = delete;
    ~iterator() override {
        if (_prev) {
            _prev->_next = _next;
        }
        if (_next) {
            _next->_prev = _prev;
        }
    }

    bool valid() const override {
        return ss::visit(
          _it,
          [](std::monostate) { return false; },
          [this](memtable::table::iterator it) {
              return it != _mem->_table.end();
          },
          [](const internal::key&) { return true; });
    }

    ss::future<> seek_to_first() override {
        _it = _mem->_table.begin();
        return ss::now();
    }

    ss::future<> seek_to_last() override {
        _it = _mem->_table.empty() ? _mem->_table.end()
                                   : std::prev(_mem->_table.end());
        return ss::now();
    }

    ss::future<> seek(lsm::internal::key_view target) override {
        _it = _mem->_table.lower_bound(target);
        return ss::now();
    }

    ss::future<> next() override {
        auto& it = restore();
        if (it != _mem->_table.end()) {
            ++it;
        }
        return ss::now();
    }

    ss::future<> prev() override {
        auto& it = restore();
        if (it == _mem->_table.begin()) {
            it = _mem->_table.end();
        } else if (it != _mem->_table.end()) {
            --it;
        } else if (!_mem->_table.empty()) {
            it = std::prev(_mem->_table.end());
        }
        return ss::now();
    }

    lsm::internal::key_view key() override { return restore()->first; }

    iobuf value() override { return restore()->second.share(); }

    void invalidate() {
        ss::visit(
          _it,
          [](std::monostate) {},
          [this](memtable::table::iterator it) {
              if (it == _mem->_table.end()) {
                  _it = std::monostate{};
              } else {
                  _it = it->first;
              }
          },
          [](const internal::key&) {});
    }

private:
    friend class memtable;

    memtable::table::iterator& restore() const {
        using iter = memtable::table::iterator;
        return *ss::visit(
          _it,
          [this](std::monostate) -> iter* {
              return &_it.emplace<iter>(_mem->_table.end());
          },
          [](memtable::table::iterator& it) -> iter* { return &it; },
          [this](const internal::key& key) -> iter* {
              return &_it.emplace<iter>(_mem->_table.find(key));
          });
    }

    iterator* _next = nullptr;
    iterator* _prev = nullptr;
    ss::lw_shared_ptr<memtable> _mem;
    mutable std::
      variant<std::monostate, memtable::table::iterator, internal::key>
        _it;
};

void memtable::add(internal::key key, iobuf value) {
    dassert(
      key.type() == internal::value_type::value,
      "when adding to the memtable, keys must be of value type",
      key.decode());
    invalidate_iterators();
    _table.emplace(std::move(key), std::move(value));
}
void memtable::remove(internal::key key) {
    dassert(
      key.type() == internal::value_type::tombstone,
      "when remove to the memtable, keys must be of tombstone type",
      key.decode());
    invalidate_iterators();
    _table.emplace(std::move(key), iobuf{});
}

std::optional<iobuf> memtable::get(internal::key_view key) {
    dassert(
      key.type() == internal::value_type::value,
      "when getting from the memtable, keys must be of value type",
      key.decode());
    auto it = _table.lower_bound(key.without_type());
    if (it != _table.end() && it->first.user_key() == key.user_key()) {
        if (it->first.type() == internal::value_type::tombstone) {
            return std::nullopt;
        }
        iobuf& v = it->second;
        return v.share();
    }
    return std::nullopt;
}

std::unique_ptr<internal::iterator> memtable::create_iterator() {
    auto it = std::make_unique<iterator>(shared_from_this());
    // Insert into our circularly linked list.
    it->_next = _list_holder->_next;
    it->_prev = _list_holder.get();
    _list_holder->_next = it.get();
    return it;
}

memtable::memtable() noexcept
  : _list_holder(std::make_unique<iterator>(iterator::dummy{})) {
    // initialize our circularly linked list.
    _list_holder->_next = _list_holder.get();
    _list_holder->_prev = _list_holder.get();
}

memtable::~memtable() = default;

void memtable::invalidate_iterators() {
    auto sentinel = _list_holder.get();
    for (auto* it = sentinel->_next; it != sentinel; it = it->_next) {
        it->invalidate();
    }
}

} // namespace lsm::db
