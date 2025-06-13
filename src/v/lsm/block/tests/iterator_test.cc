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

#include "base/seastarx.h"
#include "lsm/block/block.h"
#include "lsm/block/builder.h"
#include "test_utils/test.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

using data = std::map<std::string, std::string>;

std::unique_ptr<lsm::core::iterator> make_iterator(const data& map) {
    lsm::block::builder builder;
    for (const auto& [key, value] : map) {
        builder.add(
          lsm::core::internal_key::encode({.key = key}), iobuf::from(value));
    }
    auto b = builder.finish();
    auto c = lsm::block::contents::copy_from(b);
    return lsm::block::block(std::move(c)).create_iterator();
}

lsm::core::internal_key operator""_key(const char* str, size_t) {
    return lsm::core::internal_key::encode({.key = str});
}

} // namespace

TEST_CORO(Filter, Empty) {
    auto filter = make_iterator({});
    co_await filter->seek_to_first();
    ASSERT_FALSE_CORO(filter->valid());
    co_await filter->seek_to_last();
    ASSERT_FALSE_CORO(filter->valid());
    co_await filter->seek("foo"_key);
    ASSERT_FALSE_CORO(filter->valid());
    co_await filter->seek("bar"_key);
    ASSERT_FALSE_CORO(filter->valid());
    co_await filter->seek(""_key);
    ASSERT_FALSE_CORO(filter->valid());
}

TEST_CORO(Filter, Single) {
    auto check_first = [](lsm::core::iterator* it) -> ss::future<> {
        co_await it->seek_to_first();
        ASSERT_TRUE_CORO(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_last = [](lsm::core::iterator* it) -> ss::future<> {
        co_await it->seek_to_last();
        ASSERT_TRUE_CORO(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_at = [](lsm::core::iterator* it) -> ss::future<> {
        co_await it->seek("foo"_key);
        ASSERT_TRUE_CORO(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_before = [](lsm::core::iterator* it) -> ss::future<> {
        co_await it->seek("fo"_key);
        ASSERT_TRUE_CORO(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_after = [](lsm::core::iterator* it) -> ss::future<> {
        co_await it->seek("fooo"_key);
        EXPECT_FALSE(it->valid());
    };
    auto it = make_iterator({{"foo", "bar"}});
    for (auto& check :
         std::vector<std::function<ss::future<>(lsm::core::iterator*)>>{
           check_first,
           check_last,
           check_seek_at,
           check_seek_before,
           check_seek_after}) {
        co_await check(it.get());
        co_await check(make_iterator({{"foo", "bar"}}).get());
    }
}

MATCHER_P2(IsValid, key, value, "") {
    *result_listener << "where the key is " << key << " and the value is "
                     << value;
    if (!arg->valid()) {
        *result_listener << " but the iterator is not valid";
        return false;
    }
    if (arg->key().user_key() != key) {
        *result_listener << " but the key is " << arg->key().user_key();
        return false;
    }
    if (arg->value() != iobuf::from(value)) {
        *result_listener << " but the value is " << arg->value();
        return false;
    }
    return true;
}

TEST(Filter, FullScans) {
    std::map<std::string, std::string> data;
    for (int i = 0; i < 2000; ++i) {
        data.emplace(fmt::format("k{:04}", i), fmt::format("v{}", i));
    }
    auto it = make_iterator(data);
    it->seek_to_first().get();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->next().get();
    }
    EXPECT_FALSE(it->valid());
    it->seek_to_last().get();
    for (int i = 1999; i >= 0; --i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->prev().get();
    }
    EXPECT_FALSE(it->valid());
    it->seek_to_first().get();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->next().get();
        // Make sure we can go back
        if (it->valid()) {
            it->prev().get();
            ASSERT_THAT(
              it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
            it->next().get();
        }
    }
    EXPECT_FALSE(it->valid());
}

TEST(Filter, Seek) {
    std::map<std::string, std::string> data;
    for (int i = 0; i < 2000; ++i) {
        data.emplace(fmt::format("k{:04}", i), fmt::format("v{}", i));
    }
    auto it = make_iterator(data);
    it->seek("k100"_key).get();
    ASSERT_THAT(it, IsValid("k1000", "v1000"));
    it->seek("k050"_key).get();
    ASSERT_THAT(it, IsValid("k0500", "v500"));
    it->seek("k0333"_key).get();
    ASSERT_THAT(it, IsValid("k0333", "v333"));
    it->seek("k"_key).get();
    ASSERT_THAT(it, IsValid("k0000", "v0"));
    it->seek(""_key).get();
    ASSERT_THAT(it, IsValid("k0000", "v0"));
    it->seek("k1995"_key).get();
    ASSERT_THAT(it, IsValid("k1995", "v1995"));
}
