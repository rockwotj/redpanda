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

#include "gmock/gmock.h"
#include "lsm/utils/immutable_btree.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

#include <string>

using my_tree = lsm::utils::immutable_btree<int32_t, std::string>;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Optional;
using ::testing::Pair;

namespace {
ss::future<std::vector<std::pair<int32_t, std::string>>>
entries(const my_tree& tree) {
    auto it = tree.iterator();
    std::vector<std::pair<int32_t, std::string>> entries;
    while (auto pair = co_await it()) {
        entries.push_back(std::move(*pair));
    }
    co_return entries;
}
} // namespace

TEST(ImmutableBTree, SmokeTest) {
    my_tree tree;
    tree = tree.insert(6, "qux");
    tree = tree.insert(4, "foo");
    tree = tree.insert(2, "bar");
    EXPECT_THAT(tree.get(6), Optional(Eq("qux")));
    EXPECT_THAT(tree.get(4), Optional(Eq("foo")));
    EXPECT_THAT(tree.get(2), Optional(Eq("bar")));
    EXPECT_EQ(tree.get(0), std::nullopt);
    EXPECT_THAT(
      entries(tree).get(),
      ElementsAre(Pair(2, "bar"), Pair(4, "foo"), Pair(6, "qux")));
}
