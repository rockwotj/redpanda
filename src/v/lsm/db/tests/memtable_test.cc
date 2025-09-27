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
#include "lsm/core/internal/tests/iterator_test_harness.h"
#include "lsm/db/memtable.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string_view>

namespace {

using lsm::internal::operator""_key;

class memtable_iterator_factory {
public:
    std::unique_ptr<lsm::internal::iterator>
    make_iterator(std::map<lsm::internal::key, iobuf> map) {
        for (auto& [k, v] : map) {
            _memtable->add(k, v.copy());
        }
        return _memtable->create_iterator();
    }

    ss::lw_shared_ptr<lsm::db::memtable> _memtable
      = ss::make_lw_shared<lsm::db::memtable>();
};

} // namespace

using MemtableIteratorType = ::testing::Types<memtable_iterator_factory>;

INSTANTIATE_TYPED_TEST_SUITE_P(
  MemtableIteratorSuite, CoreIteratorTest, MemtableIteratorType);

TEST(Memtable, GetAtVersion) {
    ss::lw_shared_ptr<lsm::db::memtable> table
      = ss::make_lw_shared<lsm::db::memtable>();
    table->add("key1@1"_key, iobuf::from("value1"));
    table->add("key1@2"_key, iobuf::from("value2"));
    table->add("key1@3"_key, iobuf::from("value3"));
    table->add("key0@4"_key, iobuf::from("value4"));
    table->add("key2@5"_key, iobuf::from("value5"));
    table->add("key1@6"_key, iobuf::from("value6"));
    table->add("key3@6"_key, iobuf::from("boo!"));
    table->remove("key3@-7"_key);

    struct testcase {
        uint64_t version;
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
      {
        .version = 6,
        .key = "key3",
        .value = "boo!",
      },
      {
        .version = 7,
        .key = "key3",
      },
      {
        .version = 8,
        .key = "key3",
      },
    };

    for (const auto& tc : testcases) {
        auto key = lsm::internal::key::encode({
          .key = tc.key,
          .seq_num = lsm::internal::seqno(tc.version),
        });
        if (tc.value.empty()) {
            EXPECT_EQ(table->get(key), std::nullopt) << "key: " << key.decode();
        } else {
            auto v = iobuf::from(tc.value);
            EXPECT_THAT(table->get(key), testing::Optional(std::ref(v)))
              << "key: " << key.decode();
        }
    }
}

TEST(Memtable, StableIterator) {
    ss::lw_shared_ptr<lsm::db::memtable> table
      = ss::make_lw_shared<lsm::db::memtable>();
    table->add("key1@1"_key, iobuf::from("value1"));
    table->add("key1@2"_key, iobuf::from("value2"));
    table->add("key5@1"_key, iobuf::from("value3"));
    auto it = table->create_iterator();
    it->seek("key1@2"_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@2"_key);
    EXPECT_EQ(it->value(), iobuf::from("value2"));
    table->add("key1@3"_key, iobuf::from("value4"));
    table->add("key2@1"_key, iobuf::from("value5"));
    EXPECT_EQ(it->key(), "key1@2"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value2")) << it->value().hexdump(10);
    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@1"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value1")) << it->value().hexdump(10);
    it->prev().get();
    ASSERT_TRUE(it->valid());
    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@3"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value4")) << it->value().hexdump(10);
}
