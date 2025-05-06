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

#include <cstdint>
#include <memory>
#include <ostream>

namespace experimental::serde::json {

enum class token {
    error,
    value_null,
    value_true,
    value_false,
    value_double,
    value_int,
    value_string,
    start_object,
    key,
    end_object,
    start_array,
    end_array,
    eof,
};

std::ostream& operator<<(std::ostream& os, token t);

class parser {
public:
    explicit parser(iobuf buf);
    ~parser();

    /// Advance the parser to the next token. Returns true if the parser
    /// successfully advanced to the next token. Returns false if the
    /// parser reached the end of the input or if an error occurred.
    ss::future<bool> next();

    /// Return the current token without advancing the parser.
    token token() const;

    /// Return the current value of the parser.
    /// Can be called only if a previous call to next_token() returned
    /// token::value_int. May be called at most once.
    int64_t value_int();

    /// Return the current value of the parser.
    /// Can be called only if a previous call to next_token() returned
    /// token::value_double. May be called at most once.
    double value_double();

    /// Return the current value of the parser.
    /// Can be called only if a previous call to next_token() returned
    /// token::value_string. May be called at most once.
    iobuf value_string();

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

}; // namespace experimental::serde::json
