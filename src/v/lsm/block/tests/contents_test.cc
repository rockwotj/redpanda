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
#include "lsm/block/contents.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

// NOLINTNEXTLINE(*err58*)
static const std::filesystem::path test_path = std::getenv("TEST_TMPDIR");

TEST_CORO(Contents, StringView) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    co_await write_fully(test_path / "foo.txt", b.share(0, b.size_bytes()));
    auto file = co_await ss::open_file_dma(
      std::string(test_path / "foo.txt"), ss::open_flags::ro);
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = co_await lsm::block::contents::read(
          file, lsm::block::handle{.offset = offset, .size = buf.size_bytes()});
        std::vector<std::pair<size_t, size_t>> testcases{
          // clang-format off
          {0, 1},
          {128_KiB, 1},
          {256_KiB, 1},
          {256_KiB - 1, 1},
          {128_KiB - 1, 1},
          {256_KiB - 1, 2},
          {128_KiB - 1, 2},
          {256_KiB - 2, 4},
          {128_KiB - 2, 4},
          {1, 128_KiB},
          {2, 128_KiB},
          {2, 128_KiB-1},
          // clang-format on
        };
        for (size_t i = 0; i < testcases.size(); ++i) {
            const auto& [pos, len] = testcases[i];
            ss::sstring expected;
            for (const auto& frag : buf.share(pos, len)) {
                expected.append(frag.get(), frag.size());
            }
            auto actual = contents->read_string(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
            EXPECT_EQ(expected, ss::sstring(actual))
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
        }
    }
}

TEST_CORO(Contents, IobufShare) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    co_await write_fully(test_path / "foo.txt", b.share(0, b.size_bytes()));
    auto file = co_await ss::open_file_dma(
      std::string(test_path / "foo.txt"), ss::open_flags::ro);
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = co_await lsm::block::contents::read(
          file, lsm::block::handle{.offset = offset, .size = buf.size_bytes()});
        std::vector<std::pair<size_t, size_t>> testcases{
          // clang-format off
          {0, 1},
          {1, 2},
          {0, buf.size_bytes()},
          {buf.size_bytes(), 0},
          {buf.size_bytes() - 1, 1},
          {0, buf.size_bytes() - 1},
          {1, buf.size_bytes() - 1},
          {1, buf.size_bytes() - 2},
          {128_KiB, 128_KiB},
          {128_KiB - 1, 128_KiB + 1},
          {128_KiB + 1, 128_KiB - 1},
          {128_KiB - 1, 128_KiB},
          {128_KiB + 1, 128_KiB},
          // clang-format on
        };
        for (size_t i = 0; i < testcases.size(); ++i) {
            const auto& [pos, len] = testcases[i];
            auto expected = buf.share(pos, len);
            auto actual = contents->share(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected
              << ", actual: " << actual;
        }
    }
}
