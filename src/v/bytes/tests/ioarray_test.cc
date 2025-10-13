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

TEST(IOArray, IndexingWithOffset) {
    iobuf b;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = static_cast<char>(i);
        b.append(&c, 1);
    }

    for (auto offset : std::to_array<size_t>(
           {0, 1, 10, 100, 128_KiB - 1, 128_KiB, 128_KiB + 1, 200_KiB})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);

        for (size_t i = 0; i < std::min<size_t>(1000, contents.size()); ++i) {
            char expected = static_cast<char>(offset + i);
            char actual = contents[i];
            EXPECT_EQ(expected, actual) << "offset: " << offset << ", i: " << i;
        }

        if (contents.size() > 0) {
            contents[0] = 'X';
            EXPECT_EQ('X', contents[0]) << "offset: " << offset;
        }

        if (contents.size() > 128_KiB) {
            contents[128_KiB - 1] = 'Y';
            EXPECT_EQ('Y', contents[128_KiB - 1]) << "offset: " << offset;

            contents[128_KiB] = 'Z';
            EXPECT_EQ('Z', contents[128_KiB]) << "offset: " << offset;
        }
    }
}

TEST(IOArray, TrimBackWithOffset) {
    iobuf b;
    std::string pattern;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = 'a' + (i % 26);
        b.append(&c, 1);
        pattern.push_back(c);
    }

    for (auto offset :
         std::to_array<size_t>({1, 10, 128_KiB - 1, 128_KiB, 128_KiB + 1})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
        size_t original_size = contents.size();

        for (auto trim : std::to_array<size_t>({1, 10, 100, 1000})) {
            if (trim > original_size) continue;

            auto test_copy = ioarray::copy_from(b).share(
              offset, b.size_bytes() - offset);
            test_copy.trim_back(trim);

            EXPECT_EQ(original_size - trim, test_copy.size())
              << "offset: " << offset << ", trim: " << trim;

            std::string result;
            std::ranges::copy(test_copy.as_range(), std::back_inserter(result));
            std::string expected = pattern.substr(offset, original_size - trim);
            EXPECT_EQ(expected, result)
              << "offset: " << offset << ", trim: " << trim;
        }
    }
}

TEST(IOArray, IOVecWithOffset) {
    iobuf b;
    std::string pattern;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = 'a' + (i % 26);
        b.append(&c, 1);
        pattern.push_back(c);
    }

    for (auto offset : std::to_array<size_t>(
           {0, 1, 10, 100, 128_KiB - 1, 128_KiB, 128_KiB + 1, 200_KiB})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
        auto iov = contents.as_iovec();

        std::string result;
        size_t total_len = 0;
        for (const auto& v : iov) {
            result.append(static_cast<char*>(v.iov_base), v.iov_len);
            total_len += v.iov_len;
        }

        EXPECT_EQ(contents.size(), total_len) << "offset: " << offset;

        std::string expected = pattern.substr(offset);
        EXPECT_EQ(expected, result) << "offset: " << offset;
    }
}

TEST(IOArray, EdgeCases) {
    ioarray empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(0, empty.size());

    auto single = ioarray::copy_from(iobuf::from("X"));
    EXPECT_FALSE(single.empty());
    EXPECT_EQ(1, single.size());
    EXPECT_EQ('X', single[0]);

    auto empty_share = single.share(0, 0);
    EXPECT_TRUE(empty_share.empty());

    auto will_be_empty = ioarray::copy_from(iobuf::from("abc"));
    will_be_empty.trim_back(3);
    EXPECT_TRUE(will_be_empty.empty());
    EXPECT_EQ(0, will_be_empty.size());
}

TEST(IOArray, AlignedAllocation) {
    auto contents = ioarray::aligned(4096, 256_KiB);
    EXPECT_EQ(256_KiB, contents.size());

    for (size_t i = 0; i < contents.size(); i += 1024) {
        contents[i] = static_cast<char>(i & 0xFF);
    }

    for (size_t i = 0; i < contents.size(); i += 1024) {
        EXPECT_EQ(static_cast<char>(i & 0xFF), contents[i]);
    }
}

TEST(IOArray, StringViewComparison) {
    auto arr = ioarray::copy_from(iobuf::from("hello world"));

    auto view = arr.read_string(0, 5);
    EXPECT_EQ(view, std::string_view("hello"));
    EXPECT_NE(view, std::string_view("world"));

    auto view1 = arr.read_string(0, 5);
    auto view2 = arr.read_string(6, 5);
    EXPECT_LT(view1, std::string_view("world"));
    EXPECT_GT(view2, std::string_view("hello"));

    iobuf large_buf;
    large_buf.append_str(std::string(128_KiB - 2, 'a'));
    large_buf.append_str("xyz");
    large_buf.append_str(std::string(100, 'b'));

    auto large_arr = ioarray::copy_from(large_buf);
    auto boundary_view = large_arr.read_string(128_KiB - 2, 4);
    EXPECT_EQ(boundary_view, std::string_view("xyzb"));
    EXPECT_LT(boundary_view, std::string_view("z"));
    EXPECT_LT(std::string_view("b"), boundary_view);
    EXPECT_GT(boundary_view, std::string_view("a"));
}

TEST(IOArray, MoveSemantics) {
    auto b = ioarray::copy_from(iobuf::from("test data"));
    size_t original_size = b.size();

    auto moved = std::move(b);
    EXPECT_EQ(original_size, moved.size());

    auto b2 = ioarray::copy_from(iobuf::from("other data"));
    b2 = std::move(moved);
    EXPECT_EQ(original_size, b2.size());

    std::string result;
    std::ranges::copy(b2.as_range(), std::back_inserter(result));
    EXPECT_EQ("test data", result);
}

// NOLINTEND(*magic-numbers*)
