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
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/builder.h"
#include "test_utils/async.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {
class TableCacheTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    std::pair<lsm::internal::file_id, size_t> make_sst() {
        auto filename = lsm::internal::sst_file_name(++_latest_id);
        auto file = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(
          std::move(file), ss::make_lw_shared<lsm::internal::options>());
        // Just make empty SST files - the cache doesn't care about the contents
        builder.finish().get();
        builder.close().get();
        return std::make_pair(_latest_id, builder.file_size());
    }

    lsm::db::table_cache
    make_table_cache(size_t max_entries = default_max_entries) {
        return {
          _persistence.get(),
          max_entries,
          ss::make_lw_shared<lsm::sst::block_cache>(1_MiB)};
    }

    void TearDown() override { _persistence->close().get(); }

private:
    lsm::internal::file_id _latest_id;
    std::unique_ptr<lsm::io::persistence> _persistence
      = lsm::io::make_memory_persistence();
};

} // namespace

TEST_F(TableCacheTest, CanOpenFiles) {
    auto cache = make_table_cache();
    auto [id1, size1] = make_sst();
    auto it = cache.create_iterator(id1, size1).get();
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    it = nullptr;
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    cache.close().get();
}

TEST_F(TableCacheTest, ThrowsOnMissingFiles) {
    auto cache = make_table_cache();
    EXPECT_ANY_THROW(
      cache.create_iterator(lsm::internal::file_id{999}, 10).get());
    EXPECT_EQ(cache.statistics().open_file_handles, 0);
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntries) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_id, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    for (const auto& [id, size] : files) {
        cache.create_iterator(id, size).get();
    }
    tests::drain_task_queue().get();
    // We get 5 on the small queue (+1 over the limit) and a full ghost queue of
    // 2 entries. The main queue is empty because nothing is touched twice.
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntriesWithOpenIterators) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_id, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    std::vector<std::unique_ptr<lsm::internal::iterator>> iters;
    iters.reserve(files.size());
    for (const auto& [id, size] : files) {
        iters.push_back(cache.create_iterator(id, size).get());
    }
    // We burst over because there are open iterators. We could consider instead
    // limiting new entries to be created past the limit when there are open
    // iterators, but for now we burst.
    EXPECT_EQ(cache.statistics().open_file_handles, 20) << cache.statistics();
    iters.clear();
    tests::drain_task_queue().get();
    // But once everything is cleaned up, we only have 7 things enqueued (for
    // why 7 see the comment inMaxEntries).
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}
