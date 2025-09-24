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

#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_set.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/block_cache.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

class VersionSetTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    void TearDown() override {
        _table_cache.close().get();
        _persistence->close().get();
    }

    const lsm::internal::options& options() { return *_options; }
    lsm::db::version_set& version_set() { return _version_set; }

private:
    ss::lw_shared_ptr<lsm::internal::options> _options
      = ss::make_lw_shared<lsm::internal::options>();
    std::unique_ptr<lsm::io::persistence> _persistence
      = lsm::io::make_memory_persistence();
    lsm::db::table_cache _table_cache{
      _persistence.get(),
      default_max_entries,
      ss::make_lw_shared<lsm::sst::block_cache>(1_MiB)};
    lsm::db::version_set _version_set{
      _persistence.get(),
      &_table_cache,
      _options,
    };
};

using lsm::internal::operator""_level;

} // namespace

TEST_F(VersionSetTest, Empty) {
    auto& vset = version_set();
    for (auto level = 0_level; level <= options().default_max_level; ++level) {
        EXPECT_EQ(vset.current()->num_files(level), 0);
    }
}
