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

#include "lsm/core/keys.h"

#include "base/vassert.h"

#include <seastar/core/bitops.hh>

#include <limits>
#include <type_traits>
#include <utility>

namespace lsm::core {

core::internal_key core::internal_key::encode(parts p) {
    core::internal_key::value_t v(
      value_t::initialized_later{}, p.key.size() + 1 + sizeof(uint64_t));
    dassert(
      std::ranges::find(p.key, '\0') == p.key.end(),
      "key must not contain null characters");
    // First we append the user key.
    std::ranges::copy(p.key, v.data());
    // Then we null terminate, so that keys of different lengths compare
    // lexicographically.
    v[p.key.size()] = '\0';
    // Next we want to encode the sequence number and type, and have them sort
    // in descending order for the same key. Use 7 bytes for the offset, since
    // that should give us plenty of values before overflow.
    // Encode in BE form so the values sort lexicographically, then invert the
    // bits so they sort in descending order.
    uint64_t encoded = (static_cast<uint64_t>(p.offset()) << CHAR_WIDTH)
                       | static_cast<uint64_t>(p.type);
    encoded = ss::cpu_to_be(~encoded);
    std::memcpy(&v[p.key.size() + 1], &encoded, sizeof(encoded));
    return internal_key{v};
}

internal_key::parts internal_key::decode() const {
    parts p;
    // The user key is everything up to the last byte, which is the sequence
    // number and type.
    p.key = user_key();
    // The sequence number is the rest of the bytes, which we decode from BE
    // form and invert the bits.
    uint64_t encoded; // NOLINT
    std::memcpy(&encoded, &_value[p.key.size() + 1], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    // The last byte is the type.
    p.type = static_cast<value_type>(
      encoded & std::numeric_limits<std::underlying_type_t<value_type>>::max());
    // Shift to get back the offset, which is the rest of the bits.
    p.offset = model::offset(static_cast<int64_t>(encoded >> CHAR_WIDTH));
    return p;
}

fmt::iterator internal_key::parts::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "internal_key_parts={{key={},offset={},type={}}}",
      key,
      offset,
      std::to_underlying(type));
}

fmt::iterator internal_key::format_to(fmt::iterator it) const {
    uint64_t encoded; // NOLINT
    std::memcpy(
      &encoded, &_value[_value.size() - sizeof(encoded)], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    return fmt::format_to(
      it, "internal_key={{user={},suffix=o{:08o}}}", user_key(), encoded);
}

fmt::iterator internal_key_view::format_to(fmt::iterator it) const {
    uint64_t encoded; // NOLINT
    std::memcpy(
      &encoded, &_value[_value.size() - sizeof(encoded)], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    return fmt::format_to(
      it, "internal_key_view={{user={},suffix=o{:08o}}}", user_key(), encoded);
}

internal_key::parts
internal_key::parts::value(std::string_view key, model::offset offset) {
    return {.key = key, .offset = offset, .type = value_type::value};
}
internal_key::parts
internal_key::parts::tombstone(std::string_view key, model::offset offset) {
    return {.key = key, .offset = offset, .type = value_type::tombstone};
}
} // namespace lsm::core
