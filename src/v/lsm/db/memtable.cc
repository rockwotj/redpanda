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

class memtable::impl {
public:
    void add(internal::key key, iobuf value) {
        _table.emplace(key, std::move(value));
    }

    std::optional<iobuf> get(internal::key_view key) {
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

private:
    absl::btree_map<internal::key, iobuf, std::less<>> _table;
};

memtable::memtable() noexcept
  : _impl(std::make_unique<impl>()) {}
memtable::memtable(memtable&&) noexcept = default;
memtable& memtable::operator=(memtable&&) noexcept = default;
memtable::~memtable() = default;

void memtable::add(internal::key key, iobuf value) {
    dassert(
      key.type() == internal::value_type::value,
      "when adding to the memtable, keys must be of value type",
      key.decode());
    _impl->add(std::move(key), std::move(value));
}
void memtable::remove(internal::key key) {
    dassert(
      key.type() == internal::value_type::tombstone,
      "when remove to the memtable, keys must be of tombstone type",
      key.decode());
    _impl->add(std::move(key), {});
}

std::optional<iobuf> memtable::get(internal::key_view key) {
    dassert(
      key.type() == internal::value_type::value,
      "when getting from the memtable, keys must be of value type",
      key.decode());
    return _impl->get(key);
}

} // namespace lsm::db
