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

#include "serde/json/writer.h"

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"

namespace serde::json {

namespace {
template<typename Iter>
void serialize_json_string(Iter it, Iter end, iobuf* buf) {
    // Compile-time initialized escape lookup table for ASCII characters
    // 0 = no escape, otherwise the character to follow '\'
    constexpr size_t ascii_size = 128;
    static constinit const auto escape_table = []() consteval {
        std::array<char, ascii_size> table{};
        // Set escape characters for special cases
        table['"'] = '"';
        table['\\'] = '\\';
        table['/'] = '/';
        table['\b'] = 'b';
        table['\f'] = 'f';
        table['\n'] = 'n';
        table['\r'] = 'r';
        table['\t'] = 't';
        // All other entries default to 0 (no escaping needed)
        return table;
    }();
    buf->append_str("\"");
    auto finish_str = absl::Cleanup([&buf]() { buf->append_str("\""); });

    auto next_char = [&it]() -> char { return *it++; };
    // Process string character by character
    while (it != end) {
        auto c = static_cast<unsigned char>(next_char());
        constexpr unsigned char max_ascii_control_char = 31;
        if (c < ascii_size) {
            // NOLINTNEXTLINE(*-constant-array-index)
            char escape = escape_table[static_cast<unsigned char>(c)];
            if (escape) {
                buf->append(std::to_array({'\\', escape}));
            } else if (c <= max_ascii_control_char) {
                buf->append_str(
                  absl::StrCat(
                    "\\u",
                    absl::Hex(static_cast<int32_t>(c), absl::kZeroPad4)));
            } else {
                // Normal ASCII character, no escaping needed
                buf->append(std::to_array({c}));
            }
            continue;
        }
        // UTF-8 character handling
        uint32_t codepoint = 0;
        bool valid = false;

        auto next_char_or_invalid_utf8 = [&it, end]() -> unsigned char {
            if (it == end) {
                return '\0'; // Invalid UTF-8 sequence
            }
            return static_cast<unsigned char>(*it++);
        };
        // NOLINTBEGIN(*-magic-numbers)
        // 2-byte sequence (110xxxxx 10xxxxxx)
        if ((c & 0xE0u) == 0xC0) {
            unsigned char c1 = next_char_or_invalid_utf8();
            if ((c1 & 0xC0u) == 0x80) {
                codepoint = ((c & 0x1Fu) << 6u) | (c1 & 0x3Fu);
                // Check for overlong encoding
                valid = codepoint >= 0x80;
            }
            // 3-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
        } else if ((c & 0xF0u) == 0xE0) {
            unsigned char c1 = next_char_or_invalid_utf8();
            unsigned char c2 = next_char_or_invalid_utf8();
            if ((c1 & 0xC0u) == 0x80 && (c2 & 0xC0u) == 0x80) {
                codepoint = ((c & 0x0Fu) << 12u) | ((c1 & 0x3Fu) << 6u)
                            | (c2 & 0x3Fu);
                // Check for uTF-16 surrogates or overlong encoding
                valid = codepoint >= 0x800
                        && (codepoint < 0xD800 || codepoint > 0xDFFF);
            }
            // 4-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
        } else if ((c & 0xF8u) == 0xF0) {
            unsigned char c1 = next_char_or_invalid_utf8();
            unsigned char c2 = next_char_or_invalid_utf8();
            unsigned char c3 = next_char_or_invalid_utf8();
            if (
              (c1 & 0xC0u) == 0x80 && (c2 & 0xC0u) == 0x80
              && (c3 & 0xC0u) == 0x80) {
                codepoint = ((c & 0x07u) << 18u) | ((c1 & 0x3Fu) << 12u)
                            | ((c2 & 0x3Fu) << 6u) | (c3 & 0x3Fu);
                // Check for valid range (u+10000 through u+10FFFF)
                valid = codepoint >= 0x10000 && codepoint <= 0x10FFFF;
            }
        }

        if (!valid) {
            // Invalid uTF-8 sequence, use replacement character
            buf->append_str("\\ufffd");
            continue;
        }
        // For codepoints outside the BMP (> u+FFFF), encode as a
        // surrogate pair
        if (codepoint > 0xFFFF) {
            // Encode as surrogate pair
            uint32_t hi = 0xD800 + ((codepoint - 0x10000) >> 10u);
            uint32_t lo = 0xDC00 + ((codepoint - 0x10000) & 0x3FFu);
            buf->append_str(
              absl::StrCat(
                "\\u",
                absl::Hex(static_cast<int32_t>(hi), absl::kZeroPad4),
                "\\u",
                absl::Hex(static_cast<int32_t>(lo), absl::kZeroPad4)));
        } else {
            // BMP character, encode directly
            buf->append_str(
              absl::StrCat(
                "\\u",
                absl::Hex(static_cast<int32_t>(codepoint), absl::kZeroPad4)));
        }
        // NOLINTEND(*-magic-numbers)
    }
}
} // namespace

void writer::append_string(const iobuf& b) {
    iobuf::byte_iterator begin = {b.begin(), b.end()};
    iobuf::byte_iterator end = {b.end(), b.end()};
    serialize_json_string(begin, end, &_buf);
}

void writer::append_string(std::string_view s) {
    serialize_json_string(s.begin(), s.end(), &_buf);
}

void writer::number(double d) {
    append_delimiter();
    _buf.append_str(absl::StrCat(absl::SixDigits(d)));
    _next_delimiter = ',';
}
void writer::number(int32_t i) {
    append_delimiter();
    _buf.append_str(absl::StrCat(absl::AlphaNum(i)));
    _next_delimiter = ',';
}
void writer::number(uint32_t i) {
    append_delimiter();
    _buf.append_str(absl::StrCat(absl::AlphaNum(i)));
    _next_delimiter = ',';
}
} // namespace serde::json
