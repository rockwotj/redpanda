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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <string_view>

namespace lsm::internal {

// The sequence number for a write into the database.
using seqno = named_type<uint64_t, struct seqno_tag>;

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
//  1. The user key, which must NOT contain a `null` byte.
//  2. The sequence number, which is the offset in the log.
//  3. The value type, which is either a regular value or a tombstone.
//
// Internal keys are encoded in a way that allows them to be compared
// lexicographically, in the following manner: key ASC, offset DESC, type DESC
class key {
    constexpr static size_t sso_size = 23;
    using value_t = ss::basic_sstring<char, uint32_t, sso_size, false>;

    explicit key(value_t v)
      : _value(std::move(v)) {}

public:
    struct parts {
        ss::sstring key;
        seqno seq_num = seqno(0);
        value_type type = value_type::value;

        // Create a value internal key
        static parts value(std::string_view key, seqno);
        // Create a tombstone internal key
        static parts tombstone(std::string_view key, seqno);
        bool operator==(const parts& other) const = default;
        fmt::iterator format_to(fmt::iterator) const;
    };

    key() = default;

    // Encode a key into an internal key.
    static key encode(parts);
    // Decode a key into its parts.
    parts decode() const;
    // Returns this key's value type
    value_type type() const;

    const char& operator[](size_t i) const { return _value[i]; }
    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }
    bool empty() const { return _value.empty(); }

    // Returns the user portion of the key.
    std::string_view user_key() const {
        return {_value.data(), _value.size() - sizeof(uint64_t) - 1};
    }

    bool operator==(const key& other) const = default;
    auto operator<=>(const key&) const = default;
    bool operator<(const key&) const = default;

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class key_view;

    value_t _value;
};

// An internal key view is a lightweight view of an internal key that does not
// own the data.
class key_view {
public:
    struct parts {
        std::string_view key;
        seqno seq_num = seqno(0);
        value_type type = value_type::value;

        bool operator==(const parts& other) const = default;
        fmt::iterator format_to(fmt::iterator) const;
        explicit operator key::parts() const;
    };
    // Convert an owned key into a view.
    // NOLINTNEXTLINE(*explicit-conversions*)
    key_view(key k)
      : _value(k._value.data(), k._value.size()) {}

    // Create a view from an already encoded string.
    static key_view from_encoded(std::string_view v) { return key_view{v}; }

    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }
    bool empty() const { return _value.empty(); }

    // The user portion of the key.
    std::string_view user_key() const {
        return {_value.data(), _value.size() - sizeof(uint64_t) - 1};
    }
    // Returns this key's value type
    value_type type() const;
    // Decode a key into its parts.
    parts decode() const;

    // A key view without the tailing type marker so that the value sorts before
    // all types. This can be used to get the lexicographically first key at or
    // after a specific seqno.
    //
    // DO NOT TRY AND DECODE THIS KEY OR DO ANYTHING BUT USE IT FOR COMPARISON.
    internal::key_view without_type() const;

    // Make a copy of the view as an internal_key.
    explicit operator key() const {
        key k;
        k._value = key::value_t(_value);
        return k;
    }
    // This internal key as a string view.
    explicit operator std::string_view() const { return _value; }

    bool operator==(const key_view& other) const = default;
    auto operator<=>(const key_view&) const = default;
    bool operator<(const key_view&) const = default;
    fmt::iterator format_to(fmt::iterator) const;

private:
    explicit key_view(std::string_view v)
      : _value(v) {}

    std::string_view _value;
};

} // namespace lsm::internal
