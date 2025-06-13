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

#include "lsm/memtable.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string_view>

TEST(Memtable, GetAtVersion) {
    lsm::memtable table;
    table.add(model::offset(1), "key1", iobuf::from("value1"));
    table.add(model::offset(2), "key1", iobuf::from("value2"));
    table.add(model::offset(3), "key1", iobuf::from("value3"));
    table.add(model::offset(4), "key0", iobuf::from("value4"));
    table.add(model::offset(5), "key2", iobuf::from("value5"));
    table.add(model::offset(6), "key1", iobuf::from("value6"));

    struct testcase {
        int version;
        ss::sstring key;
        ss::sstring value;
    };

    std::vector<testcase> testcases = {
      {
        .version = 1,
        .key = "key1",
        .value = "value1",
      },
      {
        .version = 0,
        .key = "key1",
      },
      {
        .version = 2,
        .key = "key1",
        .value = "value2",
      },
      {
        .version = 3,
        .key = "key1",
        .value = "value3",
      },
      {
        .version = 4,
        .key = "key1",
        .value = "value3",
      },
      {
        .version = 6,
        .key = "key1",
        .value = "value6",
      },
      {
        .version = 99,
        .key = "key1",
        .value = "value6",
      },
      {
        .version = 4,
        .key = "key0",
        .value = "value4",
      },
      {
        .version = 5,
        .key = "key0",
        .value = "value4",
      },
      {
        .version = 3,
        .key = "key0",
      },
    };

    for (const auto& tc : testcases) {
        if (tc.value.empty()) {
            EXPECT_EQ(
              table.get(model::offset(tc.version), tc.key), std::nullopt);
        } else {
            auto v = iobuf::from(tc.value);
            EXPECT_THAT(
              table.get(model::offset(tc.version), tc.key),
              testing::Optional(std::ref(v)));
        }
    }
}
