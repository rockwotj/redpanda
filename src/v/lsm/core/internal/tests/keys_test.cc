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

#include "lsm/core/internal/keys.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {
using value_type = lsm::internal::value_type;
using key = lsm::internal::key;
using seqno = lsm::internal::sequence_number;
seqno operator""_seqno(unsigned long long seq_num) { return seqno{seq_num}; }
} // namespace

TEST(Keys, RoundTrip) {
    std::vector<key::parts> key_parts = {
      {.key = "", .seqno = 999_seqno, .type = value_type::value},
      {.key = "a", .seqno = 5_seqno, .type = value_type::value},
      {.key = "a", .seqno = 3_seqno, .type = value_type::value},
      {.key = "a", .seqno = 1_seqno, .type = value_type::value},
      {.key = "aa", .seqno = 1_seqno, .type = value_type::value},
      {.key = "a", .seqno = 1_seqno, .type = value_type::tombstone},
      {.key = "b", .seqno = 99_seqno, .type = value_type::tombstone},
      {.key = "b", .seqno = 1_seqno, .type = value_type::tombstone},
      {.key = "f", .seqno = 0_seqno, .type = value_type::value},
      {.key = "z", .seqno = 111_seqno, .type = value_type::value},
      {.key = "z", .seqno = 42_seqno, .type = value_type::tombstone},
    };
    for (const auto& part : key_parts) {
        auto encoded = key::encode(part);
        EXPECT_EQ(part, encoded.decode());
    }
}
TEST(Keys, SortCorrectly) {
    std::vector<key::parts> key_parts = {
      {.key = "", .seqno = 999_seqno, .type = value_type::value},
      {.key = "a", .seqno = 5_seqno, .type = value_type::value},
      {.key = "a", .seqno = 3_seqno, .type = value_type::value},
      {.key = "a", .seqno = 1_seqno, .type = value_type::tombstone},
      {.key = "a", .seqno = 1_seqno, .type = value_type::value},
      {.key = "aa", .seqno = 1_seqno, .type = value_type::value},
      {.key = "b", .seqno = 99_seqno, .type = value_type::tombstone},
      {.key = "b", .seqno = 1_seqno, .type = value_type::tombstone},
      {.key = "f", .seqno = 0_seqno, .type = value_type::value},
      {.key = "z", .seqno = 111_seqno, .type = value_type::value},
      {.key = "z", .seqno = 42_seqno, .type = value_type::tombstone},
    };
    std::vector<key> keys;
    keys.reserve(key_parts.size());
    for (const auto& part : key_parts) {
        keys.push_back(key::encode(part));
    }
    EXPECT_THAT(
      std::vector(keys),
      testing::WhenSorted(testing::ElementsAreArray(std::vector(keys))));
}

TEST(Keys, Operator) {
    using lsm::internal::operator""_key;
    EXPECT_EQ("foo@4"_key.decode(), key::parts::value("foo", 4_seqno));
    EXPECT_EQ("foo"_key.decode(), key::parts::value("foo", 0_seqno));
    EXPECT_EQ("bar@"_key.decode(), key::parts::value("bar", 0_seqno));
    EXPECT_EQ("foo@-9"_key.decode(), key::parts::tombstone("foo", 9_seqno));
}
