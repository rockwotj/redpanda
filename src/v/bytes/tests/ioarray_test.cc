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
#include "bytes/ioarray.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

// NOLINTBEGIN(*magic-numbers*)

TEST(IOArray, StringView) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
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
            fmt::print(std::cerr, "testcaese: offset={} i={}\n", offset, i);
            auto actual = contents.read_string(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
            EXPECT_EQ(expected, ss::sstring(actual))
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
        }
    }
}

TEST(IOArray, ShareToIOBuf) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(buf);
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
            auto actual = contents.share(pos, len).as_iobuf();
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected
              << ", actual: " << actual;
        }
    }
}

TEST(IOArray, ScatterGatherIO) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(buf);
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
        for (auto [pos, len] : testcases) {
            auto expected = buf.share(pos, len);
            auto shared = contents.share(pos, len);
            auto iov = shared.as_iovec();
            iobuf actual;
            for (auto v : iov) {
                actual.append(static_cast<char*>(v.iov_base), v.iov_len);
            }
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", pos: " << pos << ", len: " << len
              << ", expected: " << expected << ", actual: " << actual;
        }
    }
}

TEST(IOArray, Range) {
    auto b = ioarray::copy_from(iobuf::from("0123456789abcdefg"));
    std::string s;
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ("0123456789abcdefg", s);
}

TEST(IOArray, TrimBack) {
    auto b = ioarray::copy_from(iobuf::from("0123456789abcdefg"));
    b.trim_back(3);
    std::string s;
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ("0123456789abcd", s);

    std::string large_string(150_KiB, 'a');
    b = ioarray::copy_from(iobuf::from(large_string));
    b.trim_back(50_KiB);
    large_string = large_string.substr(0, 100_KiB);
    s.clear();
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ(large_string.size(), s.size());
}
