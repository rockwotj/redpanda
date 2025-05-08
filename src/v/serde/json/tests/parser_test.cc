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

#include "serde/json/parser.h"
#include "serde/json/tests/data.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace experimental::serde::json;

// Simple test to ensure the parse doesn't fail on valid sample data. The
// contents and correctness is not verified in this test.
TEST_CORO(json_test_suite, parse) {
    auto parser = experimental::serde::json::parser(
      co_await json_test_suite_sample());

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // The contents and correctness is not verified in this test.
    }

    EXPECT_EQ(parser.token(), token::eof) << "Expected to reach EOF but got: "
                                          << std::to_underlying(parser.token());
}
