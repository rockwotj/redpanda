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

#pragma once

#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

#include <string_view>

namespace lsm::core {

// The type of the key
enum class value_type : uint8_t {
    // Value is a regular value.
    value = 0,
    // Value is a tombstone.
    tombstone = 1,
};

// And internal key is an encoded key for internal DB usage.
//
// It is made up of three parts:
//  1. The user key.
//  2. The sequence number, which is the offset in the log.
//  3. The value type, which is either a regular value or a tombstone.
//
// Internal keys are encoded in a way that allows them to be compared
// lexicographically, in the following manner: key ASC, offset DESC, type DESC
class internal_key {
    constexpr static size_t sso_size = 23;
    using value_t = ss::basic_sstring<char, uint32_t, sso_size, false>;

    explicit internal_key(value_t v)
      : _value(std::move(v)) {}

public:
    struct parts {
        std::string_view key;
        model::offset offset = model::offset(0);
        value_type type = value_type::value;

        bool operator==(const parts& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, const parts&);
    };

    internal_key() = default;

    // Encode a key into an internal key.
    static internal_key encode(parts);
    // Decode a key into its parts.
    parts decode() const;

    const char& operator[](size_t i) const { return _value[i]; }
    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }

    // Returns the user portion of the key.
    std::string_view user_key() const {
        return {_value.data(), _value.size() - sizeof(uint64_t) - 1};
    }

    bool operator==(const internal_key& other) const = default;
    auto operator<=>(const internal_key&) const = default;
    bool operator<(const internal_key&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const internal_key&);

private:
    friend class internal_key_view;

    value_t _value;
};

// An internal key view is a lightweight view of an internal key that does not
// own the data.
class internal_key_view {
public:
    // Convert an owned key into a view.
    // NOLINTNEXTLINE(*explicit-conversions*)
    internal_key_view(internal_key k)
      : _value(k._value.data(), k._value.size()) {}

    // Create a view from an already encoded string.
    static internal_key_view from_encoded(std::string_view v) {
        return internal_key_view{v};
    }

    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }

    // The user portion of the key.
    std::string_view user_key() const {
        return {_value.data(), _value.size() - sizeof(uint64_t) - 1};
    }

    // Make a copy of the view as an internal_key.
    explicit operator internal_key() const {
        internal_key k;
        k._value = internal_key::value_t(_value);
        return k;
    }
    //
    explicit operator std::string_view() const { return _value; }

    bool operator==(const internal_key_view& other) const = default;
    auto operator<=>(const internal_key_view&) const = default;
    bool operator<(const internal_key_view&) const = default;

private:
    explicit internal_key_view(std::string_view v)
      : _value(v) {}

    std::string_view _value;
};

} // namespace lsm::core
