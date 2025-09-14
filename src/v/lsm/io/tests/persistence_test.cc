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

#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>

using namespace lsm::io;

using persistence_factory
  = std::function<ss::future<std::unique_ptr<persistence>>()>;

class PersistenceTest : public ::testing::TestWithParam<persistence_factory> {
protected:
    void SetUp() override { persistence = GetParam()().get(); }

    void TearDown() override {
        if (persistence) {
            persistence->close().get();
        }
    }

    ss::future<std::vector<std::string>> list_files() {
        auto gen = persistence->list_files();
        std::vector<std::string> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

    std::unique_ptr<persistence> persistence;
};

TEST_P(PersistenceTest, CanWriteAndReadAFile) {
    {
        auto w = persistence->open_sequential_writer("foo.txt").get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from("hello")).get();
        w->append(ioarray::copy_from(iobuf::from("world"))).get();
    }
    {
        auto maybe_r = persistence->open_sequential_reader("foo.txt").get();
        ASSERT_TRUE(bool(maybe_r));
        auto r = std::move(*maybe_r);
        auto _ = ss::defer([&r] { r->close().get(); });
        auto buf = r->read(4).get();
        EXPECT_EQ(buf, iobuf::from("hell")) << buf.hexdump(32);
        r->skip(1).get();
        buf = r->read(10).get();
        EXPECT_EQ(buf, iobuf::from("world")) << buf.hexdump(32);
        buf = r->read(10).get();
        EXPECT_TRUE(buf.empty()) << buf.hexdump(32);
    }
    {
        auto maybe_r = persistence->open_random_access_reader("foo.txt").get();
        ASSERT_TRUE(bool(maybe_r));
        auto r = std::move(*maybe_r);
        auto _ = ss::defer([&r] { r->close().get(); });
        auto buf = r->read(1, 4).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("ello")) << buf.hexdump(32);
        buf = r->read(5, 5).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("world")) << buf.hexdump(32);
        EXPECT_ANY_THROW(r->read(8, 4).get());
    }
}

TEST_P(PersistenceTest, ListFiles) {
    std::vector<std::string> files;
    {
        for (int i = 0; i < 25; ++i) {
            auto filename = fmt::format("foo{}.txt", i);
            files.emplace_back(filename);
            auto w = persistence->open_sequential_writer(filename).get();
            auto _ = ss::defer([&w] { w->close().get(); });
            w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
        }
    }
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
    persistence->remove_file("foo10.txt").get();
    files.erase(files.begin() + 10);
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
}

TEST_P(PersistenceTest, OverwriteFile) {
    for (int i = 0; i < 3; ++i) {
        auto w = persistence->open_sequential_writer("foo.txt").get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
    }
    auto maybe_r = persistence->open_sequential_reader("foo.txt").get();
    ASSERT_TRUE(bool(maybe_r));
    auto r = std::move(*maybe_r);
    auto _ = ss::defer([&r] { r->close().get(); });
    auto buf = r->read(20).get();
    EXPECT_EQ(buf, iobuf::from("hello, world: 2")) << buf.hexdump(32);
}

TEST_P(PersistenceTest, ReadNonExisting) {
    auto maybe_r = persistence->open_sequential_reader("foo.txt").get();
    EXPECT_FALSE(bool(maybe_r));
}

INSTANTIATE_TEST_SUITE_P(
  PersistenceSuite,
  PersistenceTest,
  testing::Values(
    [] { return ss::as_ready_future(make_memory_persistence()); },
    [] {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        // Ensure each testcase has it's own directory.
        auto subdir = ss::sstring(uuid_t::create());
        return open_disk_persistence(tmpdir / std::string_view(subdir));
    }),
  [](const testing::TestParamInfo<persistence_factory>& info) {
      switch (info.index) {
      case 0:
          return "memory";
      case 1:
          return "disk";
      default:
          return "unknown";
      }
  });
