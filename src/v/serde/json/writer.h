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

#include "bytes/iobuf.h"

namespace serde::json {

// The writer class is responsible for serializing data into JSON format.
//
// This writer can only create JSON objects, so top level objects that are
// arrays or leaf values are not supported.
//
// NOTE: This class is easily misused, it's recommended to use a DOM version
// (not yet written) or have good tests.
class writer {
public:
    void begin_object() {
        append_delimiter();
        _buf.append_str("{");
        _next_delimiter = '\0';
    }
    void end_object() {
        _buf.append_str("}");
        _next_delimiter = ',';
    }
    void key(std::string_view k) {
        append_delimiter();
        append_string(k);
        _next_delimiter = ':';
    }
    void key(const iobuf& k) {
        append_delimiter();
        append_string(k);
        _next_delimiter = ':';
    }
    void begin_array() {
        append_delimiter();
        _buf.append_str("[");
        _next_delimiter = '\0';
    }
    void end_array() {
        _buf.append_str("]");
        _next_delimiter = ',';
    }
    void null() {
        append_delimiter();
        _buf.append_str("null");
        _next_delimiter = ',';
    }
    void boolean(bool b) {
        append_delimiter();
        _buf.append_str(b ? "true" : "false");
        _next_delimiter = ',';
    }
    void string(const iobuf& b) {
        append_delimiter();
        append_string(b);
        _next_delimiter = ',';
    }
    void string(std::string_view b) {
        append_delimiter();
        append_string(b);
        _next_delimiter = ',';
    }
    void number(double d);
    void number(int32_t i);
    void number(uint32_t i);
    // NOTE: We intentionally do not support int64_t and uint64_t
    // It's not clear all JSON parsers will properly preserve those
    // types, so we force the caller to wrap that as a string like
    // proto3 json.

    iobuf&& finish() && {
        end_object();
        return std::move(_buf);
    }

private:
    void append_string(std::string_view);
    void append_string(const iobuf&);

    void append_delimiter() {
        if (_next_delimiter) {
            _buf.append_str({&_next_delimiter, 1});
        }
    }

    char _next_delimiter = '\0';
    iobuf _buf = iobuf::from("{");
};

} // namespace serde::json
