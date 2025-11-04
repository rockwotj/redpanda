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

#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/version_set.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

struct sst_spec {
    lsm::internal::file_id id;
    lsm::internal::level level;
    std::vector<lsm::internal::key> keys;
};

MATCHER_P2(IsLookupValue, key, level, "is a value") {
    auto expected = iobuf::from(
      fmt::format("value for {} on level {}", key, level));
    (*result_listener) << "equal to " << expected.hexdump(100);
    return arg == lsm::lookup_result::value(std::move(expected));
}
MATCHER(IsMissing, "is missing") { return arg.is_missing(); }
MATCHER(IsTombstone, "is a tombstone") { return arg.is_tombstone(); }

class VersionSetTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    void TearDown() override {
        _table_cache.close().get();
        _persistence->close().get();
    }

    const lsm::internal::options& options() { return *_options; }
    lsm::db::version_set& version_set() { return *_version_set; }

    void recover() {
        _version_set = nullptr;
        _version_set = ss::make_lw_shared<lsm::db::version_set>(
          _persistence.get(), &_table_cache, _options);
        _version_set->recover().get();
    }

    void add_sst(sst_spec spec) {
        auto filename = lsm::internal::sst_file_name(spec.id);
        auto writer = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(
          std::move(writer), ss::make_lw_shared<lsm::internal::options>());
        std::ranges::sort(spec.keys);
        for (const auto& key : spec.keys) {
            builder
              .add(
                key,
                iobuf::from(
                  fmt::format("value for {} on level {}", key, spec.level)))
              .get();
        }
        builder.finish().get();
        size_t file_size = builder.file_size();
        builder.close().get();
        lsm::db::version_edit edit(*_options);
        edit.add_file({
          .level = spec.level,
          .file_id = spec.id,
          .file_size = file_size,
          .smallest = spec.keys.front(),
          .largest = spec.keys.back(),
        });
        _version_set->log_and_apply(std::move(edit)).get();
    }

private:
    ss::lw_shared_ptr<lsm::internal::options> _options
      = ss::make_lw_shared<lsm::internal::options>();
    std::unique_ptr<lsm::io::persistence> _persistence
      = lsm::io::make_memory_persistence();
    lsm::db::table_cache _table_cache{
      _persistence.get(),
      default_max_entries,
      ss::make_lw_shared<lsm::sst::block_cache>(1_MiB)};
    ss::lw_shared_ptr<lsm::db::version_set> _version_set
      = ss::make_lw_shared<lsm::db::version_set>(
        _persistence.get(), &_table_cache, _options);
};

using lsm::internal::operator""_level;
using lsm::internal::operator""_file_id;
using lsm::internal::operator""_key;

} // namespace

TEST_F(VersionSetTest, Empty) {
    auto& vset = version_set();
    for (auto level = 0_level; level <= options().default_max_level; ++level) {
        EXPECT_EQ(vset.current()->num_files(level), 0);
    }
}

TEST_F(VersionSetTest, ApplyEdit) {
    auto& vset = version_set();
    lsm::db::version_edit edit(options());
    edit.add_file({
      .level = 0_level,
      .file_id = 1_file_id,
      .file_size = 100,
      .smallest = "a"_key,
      .largest = "z"_key,
    });
    vset.log_and_apply(std::move(edit)).get();
    EXPECT_EQ(vset.current()->num_files(0_level), 1);
    EXPECT_EQ(vset.current()->num_files(1_level), 0);
}

TEST_F(VersionSetTest, ApplyEditWithDelete) {
    auto& vset = version_set();
    {
        lsm::db::version_edit edit(options());
        edit.add_file({
          .level = 0_level,
          .file_id = 1_file_id,
          .file_size = 100,
          .smallest = "a"_key,
          .largest = "z"_key,
        });
        vset.log_and_apply(std::move(edit)).get();
        EXPECT_EQ(vset.current()->num_files(0_level), 1);
        EXPECT_EQ(vset.current()->num_files(1_level), 0);
    }
    lsm::db::version_edit edit(options());
    edit.remove_file(0_level, 1_file_id);
    edit.add_file({
      .level = 1_level,
      .file_id = 1_file_id,
      .file_size = 100,
      .smallest = "a"_key,
      .largest = "z"_key,
    });
    edit.add_file({
      .level = 0_level,
      .file_id = 2_file_id,
      .file_size = 80,
      .smallest = "c"_key,
      .largest = "d"_key,
    });
    vset.log_and_apply(std::move(edit)).get();
    EXPECT_EQ(vset.current()->num_files(0_level), 1);
    EXPECT_EQ(vset.current()->num_files(1_level), 1);
    EXPECT_EQ(vset.current()->num_files(2_level), 0);
}

TEST_F(VersionSetTest, Recovery) {
    {
        auto& vset = version_set();
        lsm::db::version_edit edit(options());
        edit.add_file({
          .level = 0_level,
          .file_id = 1_file_id,
          .file_size = 100,
          .smallest = "a"_key,
          .largest = "z"_key,
        });
        edit.add_file({
          .level = 0_level,
          .file_id = 2_file_id,
          .file_size = 80,
          .smallest = "c"_key,
          .largest = "d"_key,
        });
        vset.log_and_apply(std::move(edit)).get();
        EXPECT_EQ(vset.current()->num_files(0_level), 2);
        EXPECT_EQ(vset.current()->num_files(1_level), 0);
    }
    recover();
    auto& vset = version_set();
    EXPECT_EQ(vset.current()->num_files(0_level), 2);
    EXPECT_EQ(vset.current()->num_files(1_level), 0);
}

TEST_F(VersionSetTest, OverlapInLevel0) {
    auto& vset = version_set();
    lsm::db::version_edit edit(options());
    edit.add_file({
      .level = 0_level,
      .file_id = 1_file_id,
      .file_size = 100,
      .smallest = "d"_key,
      .largest = "g"_key,
    });
    edit.add_file({
      .level = 0_level,
      .file_id = 2_file_id,
      .file_size = 80,
      .smallest = "i"_key,
      .largest = "k"_key,
    });
    edit.add_file({
      .level = 0_level,
      .file_id = 3_file_id,
      .file_size = 80,
      .smallest = "b"_key,
      .largest = "e"_key,
    });
    vset.log_and_apply(std::move(edit)).get();
    auto current = vset.current();
    EXPECT_TRUE(current->overlap_in_level(0_level, "a"_key, "z"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "k"_key, "l"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "f"_key, "h"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "h"_key, "j"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "g"_key, "h"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "h"_key, "i"_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, std::nullopt, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(0_level, "k"_key, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(0_level, std::nullopt, "d"_key));
    EXPECT_FALSE(current->overlap_in_level(0_level, std::nullopt, "a"_key));
    EXPECT_FALSE(current->overlap_in_level(0_level, "l"_key, std::nullopt));
    EXPECT_FALSE(current->overlap_in_level(0_level, "a"_key, "a"_key));
    EXPECT_FALSE(current->overlap_in_level(0_level, "y"_key, "z"_key));
    EXPECT_FALSE(current->overlap_in_level(0_level, "h"_key, "h"_key));
    EXPECT_FALSE(current->overlap_in_level(1_level, "a"_key, "z"_key));
}

TEST_F(VersionSetTest, OverlapInLevel1) {
    auto& vset = version_set();
    lsm::db::version_edit edit(options());
    edit.add_file({
      .level = 1_level,
      .file_id = 1_file_id,
      .file_size = 100,
      .smallest = "d"_key,
      .largest = "g"_key,
    });
    edit.add_file({
      .level = 1_level,
      .file_id = 2_file_id,
      .file_size = 80,
      .smallest = "i"_key,
      .largest = "k"_key,
    });
    vset.log_and_apply(std::move(edit)).get();
    auto current = vset.current();
    EXPECT_TRUE(current->overlap_in_level(1_level, "a"_key, "z"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "k"_key, "l"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "f"_key, "h"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "h"_key, "j"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "g"_key, "h"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "h"_key, "i"_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, std::nullopt, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(1_level, "k"_key, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(1_level, std::nullopt, "d"_key));
    EXPECT_FALSE(current->overlap_in_level(1_level, std::nullopt, "c"_key));
    EXPECT_FALSE(current->overlap_in_level(1_level, "l"_key, std::nullopt));
    EXPECT_FALSE(current->overlap_in_level(1_level, "a"_key, "b"_key));
    EXPECT_FALSE(current->overlap_in_level(1_level, "y"_key, "z"_key));
    EXPECT_FALSE(current->overlap_in_level(1_level, "h"_key, "h"_key));
    EXPECT_FALSE(current->overlap_in_level(2_level, "a"_key, "z"_key));
}

TEST_F(VersionSetTest, Get) {
    add_sst({
      .id = 1_file_id,
      .level = 2_level,
      .keys = {
        "b"_key,
        "c"_key,
        "d"_key,
        "e"_key,
      },
    });
    add_sst({
      .id = 2_file_id,
      .level = 1_level,
      .keys = {
        "a"_key,
        "b"_key,
        "c"_key,
        "d"_key,
      },
    });
    add_sst({
      .id = 3_file_id,
      .level = 1_level,
      .keys = {
        "w"_key,
        "x"_key,
        "y"_key,
        "z"_key,
      },
    });
    auto& vset = version_set();
    lsm::db::version::get_stats stats;
    auto result = vset.current()->get("a"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("a"_key, 1_level));
    result = vset.current()->get("e"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("e"_key, 2_level));
    result = vset.current()->get("b"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("b"_key, 1_level));
    result = vset.current()->get("j"_key, &stats).get();
    EXPECT_THAT(result, IsMissing());
}
