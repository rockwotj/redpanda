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

#include "lsm/memtable.h"

#include "absl/container/btree_map.h"

#include <compare>

namespace lsm {

namespace {
struct key_version_view {
    std::string_view key;
    model::offset version;

    bool operator==(const key_version_view& other) const = default;
};

struct key_version {
    ss::sstring key;
    model::offset version;

    // NOLINTNEXTLINE(*explicit-conversion*)
    operator key_version_view() const {
        return {.key = key, .version = version};
    }

    bool operator==(const key_version& other) const = default;
};

struct key_version_compare {
    using is_transparent = void;

    bool
    operator()(const key_version_view& lhs, const key_version_view& rhs) const {
        // Sort by key first, then by version in descending order
        // Do this so that we can easily lookup the most recent key for any
        // version.
        auto key_cmp = lhs.key <=> rhs.key;
        if (key_cmp != std::strong_ordering::equal) {
            return key_cmp == std::strong_ordering::less;
        }
        return lhs.version > rhs.version;
    }
};

} // namespace

class memtable::impl {
public:
    void add(model::offset offset, ss::sstring key, iobuf value) {
        _table.emplace(
          key_version{.key = std::move(key), .version = offset},
          std::move(value));
    }

    std::optional<iobuf> get(model::offset o, std::string_view key) {
        auto it = _table.lower_bound(
          key_version_view{.key = key, .version = o});
        if (it != _table.end() && it->first.key == key) {
            iobuf& v = it->second;
            return v.share(0, v.size_bytes());
        }
        return std::nullopt;
    }

private:
    absl::btree_map<key_version, iobuf, key_version_compare> _table;
};

memtable::memtable() noexcept
  : _impl(std::make_unique<impl>()) {}
memtable::memtable(memtable&&) noexcept = default;
memtable& memtable::operator=(memtable&&) noexcept = default;
memtable::~memtable() = default;

void memtable::add(model::offset offset, ss::sstring key, iobuf value) {
    _impl->add(offset, std::move(key), std::move(value));
}

std::optional<iobuf> memtable::get(model::offset o, std::string_view key) {
    return _impl->get(o, key);
}

} // namespace lsm
