/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * This file includes code from RapidJSON (https://rapidjson.org/)
 *
 * Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
 *
 * Licensed under the MIT License (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "serde/json/detail/string.h"

namespace {

// RapidJSON escape character table.
#define Z16 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
constexpr char escape[256] = {
  Z16,  Z16, 0,    0,   '\"', 0,   0,    0,   0,    0,   0,    0,  0, 0, 0, 0,
  0,    '/', Z16,  Z16, 0,    0,   0,    0,   0,    0,   0,    0,  0, 0, 0, 0,
  '\\', 0,   0,    0,   0,    0,   '\b', 0,   0,    0,   '\f', 0,  0, 0, 0, 0,
  0,    0,   '\n', 0,   0,    0,   '\r', 0,   '\t', 0,   0,    0,  0, 0, 0, 0,
  0,    0,   0,    0,   Z16,  Z16, Z16,  Z16, Z16,  Z16, Z16,  Z16};
#undef Z16

// RapidJSON codepoint conversion.
static void append_codepoint(iobuf& buf, unsigned codepoint) {
    auto append = [&buf](char c) { buf.append(&c, 1); };

    if (codepoint <= 0x7F) {
        append(static_cast<char>(codepoint & 0xFF));
    } else if (codepoint <= 0x7FF) {
        append(static_cast<char>(0xC0 | ((codepoint >> 6) & 0xFF)));
        append(static_cast<char>(0x80 | ((codepoint & 0x3F))));
    } else if (codepoint <= 0xFFFF) {
        append(static_cast<char>(0xE0 | ((codepoint >> 12) & 0xFF)));
        append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        append(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
        // RAPIDJSON_ASSERT(codepoint <= 0x10FFFF);
        append(static_cast<char>(0xF0 | ((codepoint >> 18) & 0xFF)));
        append(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
        append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        append(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
}

// RapidJSON codepoint parsing.
static unsigned parse_codepoint(const char* buf) {
    unsigned codepoint = 0;
    for (size_t i = 0; i < 4; ++i) {
        codepoint <<= 4;
        if (buf[i] >= '0' && buf[i] <= '9') {
            codepoint |= (buf[i] - '0');
        } else if (buf[i] >= 'a' && buf[i] <= 'f') {
            codepoint |= (buf[i] - 'a' + 10);
        } else if (buf[i] >= 'A' && buf[i] <= 'F') {
            codepoint |= (buf[i] - 'A' + 10);
        }
    }
    return codepoint;
}

} // namespace

namespace experimental::serde::json::detail {

size_t string_parser::advance(
  ss::temporary_buffer<char>& buf, string_parser::result& err) {
    size_t start = 0;
    size_t pos = 0;

    auto do_sink_raw = [&](bool exclude_last) {
        // TODO: Consider zero-copy for large strings. `buf.share()` has an
        //   allocation overhead so it is not always the best option.
        auto sz = pos - start - exclude_last;
        if (sz > 0) {
            _sink.append(buf.get() + start, sz);
        }
    };

    while (pos < buf.size()) {
        switch (_state) {
        case state::finished_with_error:
            throw std::runtime_error(
              "string_parser is in error state and is not reusable");
        case state::finished_with_value:
            throw std::runtime_error(
              "string_parser is already done and is not reusable");

        case state::start:
            // Expect the " (start of string) character.
            if (buf.empty()) {
                break;
            } else if (buf[0] == '"') {
                _state = state::in_string;
                start = 1;
                pos = 1;
                continue;
            }

            err = result::invalid_json_string;
            _state = state::finished_with_error;
            return pos + 1;

        case state::in_string: {
            auto c = buf[pos];
            pos += 1;

            if (c == '\\') {
                // Copy what we have so far.
                do_sink_raw(true);
                start = pos;
                _state = state::in_escape;
                continue;
            } else if (c == '"') {
                _state = state::finished_with_value;
                err = result::done;
                // Exclude the closing quote.
                do_sink_raw(true);
                return pos;
            } else if (unsigned(c) < 0x20) {
                // Invalid character in string.
                // RFC 4627: unescaped = %x20-21 / %x23-5B / %x5D-10FFFF
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }
            break;
        }

        case state::in_escape: {
            auto c = buf[pos];
            pos += 1;

            if (unsigned(c) < 256 && escape[static_cast<unsigned char>(c)]) {
                // Copy the escaped character.
                _sink.append(&escape[static_cast<unsigned char>(c)], 1);
                start = pos;
                _state = state::in_string;
                continue;
            } else if (c == 'u') {
                _state = state::in_unicode;
                start = pos;
                continue;
            }

            throw std::runtime_error(fmt::format("invalid escape: {}", c));
        }

        case state::in_unicode: {
            auto c = buf[pos];
            pos += 1;

            if (c >= '0' && c <= '9') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else if (c >= 'a' && c <= 'f') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else if (c >= 'A' && c <= 'F') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            if (_unicode_index == 4) {
                unsigned codepoint = parse_codepoint(_unicode_buffer.data());
                if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
                    // High surrogate, check if followed by valid low
                    // surrogate.
                    if (codepoint <= 0xDBFF) {
                        _state = state::surrogate_start;
                        continue;
                    } else {
                        // Single low surrogate.
                        err = result::invalid_json_string;
                        _state = state::finished_with_error;
                        return pos;
                    }
                } else {
                    append_codepoint(_sink, codepoint);
                    _unicode_index = 0;
                    _state = state::in_string;
                    start = pos;
                    continue;
                }
            } else if (_unicode_index == 8) {
                auto codepoint = parse_codepoint(_unicode_buffer.data());
                auto codepoint2 = parse_codepoint(_unicode_buffer.data() + 4);

                if (codepoint2 < 0xDC00 || codepoint2 > 0xDFFF) {
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }

                codepoint = (((codepoint - 0xD800) << 10)
                             | (codepoint2 - 0xDC00))
                            + 0x10000;

                append_codepoint(_sink, codepoint);
                _unicode_index = 0;
                _state = state::in_string;
                start = pos;
                continue;
            }

            break;
        }

        case state::surrogate_start: {
            auto c = buf[pos];
            pos += 1;

            if (c != '\\') {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            _state = state::surrogate_escape;
            start = pos;
            continue;
        }

        case state::surrogate_escape: {
            auto c = buf[pos];
            pos += 1;

            if (c != 'u') {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            _state = state::in_unicode;
            start = pos;
            continue;
        }
        }
    }

    do_sink_raw(false);

    err = result::need_more_data;
    return pos;
}

} // namespace experimental::serde::json::detail
