/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "serde/protobuf/json.h"

#include "absl/strings/numbers.h"
#include "base/units.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace serde::pb::json {

using serde::json::token;

peekable_parser::peekable_parser(iobuf&& buf, serde::json::parser_config config)
  : _parser(std::move(buf), config) {}

ss::future<token> peekable_parser::peek() {
    if (_peeked_token.has_value()) {
        co_return _peeked_token.value();
    }
    if (co_await _parser.next()) {
        _peeked_token = _parser.token();
    }
    co_return _parser.token();
}

ss::future<bool> peekable_parser::next() {
    if (_peeked_token) {
        _peeked_token.reset();
        co_return true;
    }
    co_return co_await _parser.next();
}

namespace {

ss::future<> expect_token(peekable_parser* parser, token expected) {
    co_await parser->next();
    if (parser->token() != expected) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected {}, got: {}", expected, parser->token()));
    }
}

} // namespace

ss::future<std::optional<iobuf>> object_key_generator::operator()() {
    if (_begin) {
        _begin = false;
        co_await expect_token(_parser, token::start_object);
    }
    co_await _parser->next();
    if (_parser->token() == token::end_object) {
        co_return std::nullopt;
    } else if (_parser->token() != token::key) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected object key, got: {}", _parser->token()));
    }
    co_return _parser->value_string();
}

ss::future<bool> array_element_generator::operator()() {
    if (_begin) {
        _begin = false;
        co_await expect_token(_parser, token::start_array);
    }
    if (co_await _parser->peek() == token::end_array) {
        co_await _parser->next();
        co_return false;
    }
    co_return true;
}

ss::future<> check_next_eof(peekable_parser* parser) {
    auto has_next = co_await parser->next();
    if (!has_next) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("next token, parser state: {}", parser->token()));
    }
    if (parser->token() == token::eof) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected eof, got: {}", parser->token()));
    }
}

void check_error(peekable_parser* parser) {
    if (parser->token() == token::error) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected no error, got: {}", parser->token()));
    }
}

namespace {
constexpr static size_t max_numeric_string_size = 128;

ss::sstring linearize_iobuf(iobuf b) {
    constexpr static size_t max_allocation_size = 128_KiB;
    if (b.size_bytes() > max_allocation_size) {
        throw std::runtime_error(fmt::format(
          "string too big: {} > {}", b.size_bytes(), max_allocation_size));
    }
    ss::sstring result{ss::sstring::initialized_later{}, b.size_bytes()};
    char* dest = result.data();
    for (const auto& frag : b) {
        dest = std::copy_n(frag.get(), frag.size(), dest);
    }
    return result;
}

template<typename T>
T iobuf_to_number(iobuf b) {
    if (b.size_bytes() > max_numeric_string_size) {
        throw std::runtime_error(fmt::format(
          "number too big: {} > {}", b.size_bytes(), max_numeric_string_size));
    }
    auto str = linearize_iobuf(std::move(b));
    T result{};
    if constexpr (std::is_floating_point_v<T>) {
        bool ok{};
        if constexpr (std::is_same_v<T, float>) {
            ok = absl::SimpleAtof(str, &result);
        } else {
            ok = absl::SimpleAtod(str, &result);
        }
        if (!ok) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to parse number from string: {}", str));
        }
    } else {
        if (!absl::SimpleAtoi<T>(str, &result)) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to parse number from string: {}", str));
        }
    }
    return result;
}

template<typename T>
T read_number(peekable_parser* parser) {
    if (parser->token() == token::value_string) {
        return iobuf_to_number<T>(parser->value_string());
    }
    if (parser->token() == token::value_double) {
        return static_cast<T>(parser->value_double());
    }
    if (parser->token() != token::value_int) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected number, got: {}", parser->token()));
    }
    return static_cast<T>(parser->value_int());
}

} // namespace

template<>
bool transform_map_key(iobuf key_string) {
    if (key_string == "true") {
        return true;
    } else if (key_string == "false") {
        return false;
    } else {
        constexpr size_t hexdump_size = 32;
        throw std::runtime_error(fmt::format(
          "expected boolean, got: {}", key_string.hexdump(hexdump_size)));
    }
}

template<>
int32_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<int32_t>(std::move(key_string));
}

template<>
int64_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<int64_t>(std::move(key_string));
}

template<>
uint32_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<uint32_t>(std::move(key_string));
}

template<>
uint64_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<uint64_t>(std::move(key_string));
}

template<>
float transform_map_key(iobuf key_string) {
    return iobuf_to_number<float>(std::move(key_string));
}

template<>
double transform_map_key(iobuf key_string) {
    return iobuf_to_number<double>(std::move(key_string));
}

template<>
ss::sstring transform_map_key(iobuf key_string) {
    return linearize_iobuf(std::move(key_string));
}

bool read_bool(peekable_parser* parser) {
    switch (parser->token()) {
    case json::token::value_true:
        return true;
    case json::token::value_false:
        return false;
    default:
        throw std::runtime_error(
          fmt::format("expected boolean, got: {}", parser->token()));
    }
}
int32_t read_int32(peekable_parser* parser) {
    return read_number<int32_t>(parser);
}
int64_t read_int64(peekable_parser* parser) {
    return read_number<int64_t>(parser);
}
uint32_t read_uint32(peekable_parser* parser) {
    return read_number<uint32_t>(parser);
}
uint64_t read_uint64(peekable_parser* parser) {
    return read_number<uint64_t>(parser);
}
float read_float(peekable_parser* parser) { return read_number<float>(parser); }
double read_double(peekable_parser* parser) {
    return read_number<double>(parser);
}
ss::sstring read_string(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return linearize_iobuf(parser->value_string());
}
iobuf read_string_as_bytes(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return parser->value_string();
}

iobuf read_base64_encoded_bytes(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return base64_to_iobuf(parser->value_string());
}

} // namespace serde::pb::json
