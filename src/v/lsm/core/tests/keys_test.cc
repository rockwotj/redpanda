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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {
model::offset operator""_o(unsigned long long offset) {
    return model::offset{static_cast<int64_t>(offset)};
}
} // namespace

TEST(Keys, RoundTrip) {
    std::vector<lsm::core::internal_key::parts> key_parts = {
      {.key = "", .offset = 999_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 5_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 3_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 1_o, .type = lsm::core::value_type::value},
      {.key = "aa", .offset = 1_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 1_o, .type = lsm::core::value_type::tombstone},
      {.key = "b", .offset = 99_o, .type = lsm::core::value_type::tombstone},
      {.key = "b", .offset = 1_o, .type = lsm::core::value_type::tombstone},
      {.key = "f", .offset = 0_o, .type = lsm::core::value_type::value},
      {.key = "z", .offset = 111_o, .type = lsm::core::value_type::value},
      {.key = "z", .offset = 42_o, .type = lsm::core::value_type::tombstone},
    };
    for (const auto& part : key_parts) {
        auto encoded = lsm::core::internal_key::encode(part);
        EXPECT_EQ(part, encoded.decode());
    }
}
TEST(Keys, SortCorrectly) {
    std::vector<lsm::core::internal_key::parts> key_parts = {
      {.key = "", .offset = 999_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 5_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 3_o, .type = lsm::core::value_type::value},
      {.key = "a", .offset = 1_o, .type = lsm::core::value_type::tombstone},
      {.key = "a", .offset = 1_o, .type = lsm::core::value_type::value},
      {.key = "aa", .offset = 1_o, .type = lsm::core::value_type::value},
      {.key = "b", .offset = 99_o, .type = lsm::core::value_type::tombstone},
      {.key = "b", .offset = 1_o, .type = lsm::core::value_type::tombstone},
      {.key = "f", .offset = 0_o, .type = lsm::core::value_type::value},
      {.key = "z", .offset = 111_o, .type = lsm::core::value_type::value},
      {.key = "z", .offset = 42_o, .type = lsm::core::value_type::tombstone},
    };
    std::vector<lsm::core::internal_key> keys;
    keys.reserve(key_parts.size());
    for (const auto& part : key_parts) {
        keys.push_back(lsm::core::internal_key::encode(part));
    }
    EXPECT_THAT(
      std::vector(keys),
      testing::WhenSorted(testing::ElementsAreArray(std::vector(keys))));
}
