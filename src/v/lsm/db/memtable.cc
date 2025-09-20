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

namespace lsm::db {

// TODO(lsm): This needs to handle iterator invalidation
class iterator : public internal::iterator {
public:
    explicit iterator(memtable::table* table)
      : _table(table)
      , _it(table->end()) {}

    bool valid() const override { return _it != _table->end(); }

    ss::future<> seek_to_first() override {
        _it = _table->begin();
        return ss::now();
    }

    ss::future<> seek_to_last() override {
        _it = _table->empty() ? _table->end() : std::prev(_table->end());
        return ss::now();
    }

    ss::future<> seek(lsm::internal::key_view target) override {
        _it = _table->lower_bound(lsm::internal::key(target));
        return ss::now();
    }

    ss::future<> next() override {
        if (_it != _table->end()) {
            ++_it;
        }
        return ss::now();
    }

    ss::future<> prev() override {
        if (_it == _table->begin()) {
            _it = _table->end();
        } else if (_it != _table->end()) {
            --_it;
        } else if (!_table->empty()) {
            _it = std::prev(_table->end());
        }
        return ss::now();
    }

    lsm::internal::key_view key() override { return _it->first; }

    iobuf value() override { return _it->second.share(); }

private:
    memtable::table* _table;
    memtable::table::iterator _it;
};

void memtable::add(internal::key key, iobuf value) {
    dassert(
      key.type() == internal::value_type::value,
      "when adding to the memtable, keys must be of value type",
      key.decode());
    _table.emplace(std::move(key), std::move(value));
}
void memtable::remove(internal::key key) {
    dassert(
      key.type() == internal::value_type::tombstone,
      "when remove to the memtable, keys must be of tombstone type",
      key.decode());
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
    return std::make_unique<iterator>(&_table);
}

} // namespace lsm::db
