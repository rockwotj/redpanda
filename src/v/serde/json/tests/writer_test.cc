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

#include "serde/json/writer.h"

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include <numbers>

namespace {

std::string to_string(serde::json::writer&& w) {
    iobuf b = std::move(w).finish();
    std::string output;
    for (const auto& frag : b) {
        output.append(frag.get(), frag.size());
    }
    return output;
}

class JsonWriterTest : public ::testing::Test {
protected:
    std::string serialize(const std::function<void(serde::json::writer&)>& fn) {
        serde::json::writer w;
        fn(w);
        return to_string(std::move(w));
    }
};

TEST_F(JsonWriterTest, EscapedBackslashAndQuotes) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("key");
        w.string(R"(value with "escaped quote" and backslash\)");
    });
    EXPECT_EQ(
      result, R"({"key":"value with \"escaped quote\" and backslash\\"})");
}

TEST_F(JsonWriterTest, UnicodeEscapeSequences) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("key");
        w.string("\u2028\u2029\xF0\x9D\x84\x9E"); // UTF-8 for U+1D11E
    });
    EXPECT_EQ(result, R"({"key":"\u2028\u2029\ud834\udd1e"})");
    result = serialize([](serde::json::writer& w) {
        w.key("emoji");
        w.string("\u2028\u2029\xF0\x9D\x84\x9E"); // UTF-8 for U+1D11E
    });
}

TEST_F(JsonWriterTest, Utf8CharacterWidths) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("utf8");
        // 1-byte: A, 2-byte: ©, 3-byte: €, 4-byte: 😀
        w.string("A\xC2\xA9\xE2\x82\xAC\xF0\x9F\x98\x80");
    });
    EXPECT_EQ(result, R"({"utf8":"A\u00a9\u20ac\ud83d\ude00"})");
}

TEST_F(JsonWriterTest, InvalidUtf8) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("utf8");
        // Should be an invalid utf8 character, so we get the replacement char
        w.string("My face was: \xF0\x9F\x98 - can't you see the smile?");
    });
    EXPECT_EQ(
      result, R"({"utf8":"My face was: \ufffd- can't you see the smile?"})");
}

TEST_F(JsonWriterTest, EmptyKey) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("");
        w.string("empty key");
    });
    EXPECT_EQ(result, "{\"\":\"empty key\"}");
}

TEST_F(JsonWriterTest, KeyWithEscapedChars) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("ke\ny");
        w.string(std::string_view{"val\0ue", 6});
    });
    EXPECT_EQ(result, R"({"ke\ny":"val\u0000ue"})");
}

TEST_F(JsonWriterTest, DeeplyNestedObject) {
    std::string expected = "{";
    for (char c = 'a'; c <= 'g'; ++c) {
        expected += std::string("\"") + c + "\":{";
    }
    expected.pop_back();
    expected += R"("too deep?")";
    for (int i = 0; i <= ('g' - 'a'); ++i) {
        expected += "}";
    }

    auto result = serialize([](serde::json::writer& w) {
        w.key("a");
        w.begin_object();
        auto o1 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("b");
        w.begin_object();
        auto o2 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("c");
        w.begin_object();
        auto o3 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("d");
        w.begin_object();
        auto o4 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("e");
        w.begin_object();
        auto o5 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("f");
        w.begin_object();
        auto o6 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("g");
        w.string("too deep?");
    });
    EXPECT_EQ(result, expected);
}

TEST_F(JsonWriterTest, Primatives) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("pi");
        w.number(std::numbers::pi);
        w.key("null");
        w.null();
        w.key("boolean");
        w.boolean(true);
        w.key("integer");
        w.number(42);
        w.key("array");
        w.begin_array();
        w.number(std::numbers::e);
        w.boolean(false);
        w.null();
        w.number(-42);
        w.end_array();
    });
    EXPECT_EQ(
      result,
      R"({"pi":3.14159,"null":null,"boolean":true,"integer":42,"array":[2.71828,false,null,-42]})");
}

TEST_F(JsonWriterTest, EmptyArrayAndObject) {
    auto result = serialize([](serde::json::writer& w) {
        w.key("emptyArr");
        w.begin_array();
        w.end_array();
        w.key("emptyObj");
        w.begin_object();
        w.end_object();
    });
    EXPECT_EQ(result, R"({"emptyArr":[],"emptyObj":{}})");
}

} // namespace
