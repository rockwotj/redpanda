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

#include "base/format_to.h"
#include "gtest/gtest.h"
#include "serde/protobuf/field_mask.h"
#include "src/v/serde/protobuf/tests/codegen_test.proto.h"
#include "src/v/serde/protobuf/tests/test_messages_edition2023.proto.h"

using namespace serde::pb;

TEST(FieldMaskValidationTest, SingleField) {
    using proto_t = proto::example::super_duper_secret;
    EXPECT_TRUE(field_mask{}.is_valid_for_message<proto_t>());
    EXPECT_TRUE(
      field_mask{.paths = {{"value"}}}.is_valid_for_message<proto_t>());
    EXPECT_FALSE(
      field_mask{.paths = {{"val"}}}.is_valid_for_message<proto_t>());
}

TEST(FieldMaskValidationTest, Recursive) {
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    field_mask::path path = {};
    EXPECT_TRUE(field_mask{{path}}.is_valid_for_message<proto_t>());
    for (int i = 0; i < 10; ++i) {
        path.emplace_back("optional_nested_message");
        EXPECT_TRUE(field_mask{{path}}.is_valid_for_message<proto_t>())
          << fmt::format("{}", path);
        path.emplace_back("corecursive");
        EXPECT_TRUE(field_mask{{path}}.is_valid_for_message<proto_t>())
          << fmt::format("{}", path);
    }
}

TEST(FieldMaskValidationTest, WellKnown) {
    using proto_t = proto::example::well_known_protos;
    field_mask mask;
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = {.paths = {{"single_duration"}}};
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = field_mask{.paths = {{"single_duration", "nanos"}}};
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>());
}

TEST(FieldMaskValidationTest, Repeated) {
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    field_mask mask{
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
      },
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = {
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
        {"repeated_nested_message"},
      },
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = {
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
        {"repeated_nested_message", "a"},
      },
    };
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>());
}

TEST(FieldMaskValidationTest, Map) {
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    field_mask mask{
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
      },
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>())
      << fmt::format("{}", mask);
    mask = {
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
        {"map_string_nested_message"},
      },
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>())
      << fmt::format("{}", mask);
    mask = {
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
        {"map_string_nested_message", "key"},
      },
    };
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>())
      << fmt::format("{}", mask);
    mask = {
      .paths = {
        {"optional_nested_message"},
        {"optional_int32"},
        {"repeated_int32"},
        {"map_string_nested_message", "key", "a"},
      },
    };
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>())
      << fmt::format("{}", mask);
}

TEST(FieldMaskValidationTest, Oneof) {
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    field_mask mask{
      .paths = {{"oneof_uint32"}},
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = {
      .paths = {{"oneof_nested_message", "a"}},
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
    mask = {
      .paths = {{"oneof_nested_message", "b"}},
    };
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>());
    // It would be nice to validate that oneof fields are set mutually
    // exclusively, but for now we just do the "normal" protobuf thing of last
    // one wins.
    mask = {
      .paths = {{"oneof_double"}, {"oneof_uint32"}},
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
}

TEST(FieldMaskValidationTest, Overlap) {
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    field_mask mask = {
      .paths = {
        {"recursive_message"},
        {"recursive_message", "optional_bool"},
      },
    };
    EXPECT_FALSE(mask.is_valid_for_message<proto_t>());
    mask = {
      .paths = {
        {"recursive_message", "optional_string"},
        {"recursive_message", "optional_bool"},
      },
    };
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>());
}

struct merge_test_case {
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", name);
    }

    std::string name;
    std::string input;
    std::string update;
    std::string mask;
    std::string expected;
};

class FieldMaskMergeParameterizedTest
  : public ::testing::TestWithParam<merge_test_case> {};

TEST_P(FieldMaskMergeParameterizedTest, Merge) {
    const merge_test_case& test_case = GetParam();
    using proto_t
      = protobuf_test_messages::editions::test_all_types_edition2023;
    auto actual = proto_t::from_json(iobuf::from(test_case.input)).get();
    auto update = proto_t::from_json(iobuf::from(test_case.update)).get();
    auto expected = proto_t::from_json(iobuf::from(test_case.expected)).get();
    field_mask mask = proto::example::mask_wrapper::from_json(
                        iobuf::from(test_case.mask))
                        .get()
                        .get_mask();
    EXPECT_TRUE(mask.is_valid_for_message<proto_t>()) << mask;
    mask.merge_into(std::move(update), &actual);
    EXPECT_EQ(expected, actual);
}

INSTANTIATE_TEST_SUITE_P(
  MergeTestCases,
  FieldMaskMergeParameterizedTest,
  ::testing::Values(
    merge_test_case{
      .name = "SingleField",
      .input = R"({"optionalInt32": 1, "optionalBool": true})",
      .update = R"({"optionalInt32": 2})",
      .mask = R"({"mask": "optionalInt32"})",
      .expected = R"({"optionalInt32": 2, "optionalBool": true})",
    },
    merge_test_case{
      .name = "ExtraUpdateFields",
      .input = R"({"optionalInt32": 1})",
      .update = R"({"optionalInt32": 3, "optionalBool":true})",
      .mask = R"({"mask": "optionalBool"})",
      .expected = R"({"optionalInt32": 1, "optionalBool":true})",
    },
    merge_test_case{
      .name = "RepeatedField",
      .input = R"({"repeatedInt32": [1], "optionalBool": true})",
      .update = R"({"repeatedInt32": [3]})",
      .mask = R"({"mask": "repeatedInt32"})",
      .expected = R"({"repeatedInt32": [1, 3], "optionalBool":true})",
    },
    merge_test_case{
      .name = "MapField",
      .input = R"({"mapInt32Int32": {"1":5,"2":8}, "optionalString": "foo"})",
      .update = R"({"mapInt32Int32": {"1":6,"3":9}, "optionalString": "bar"})",
      .mask = R"({"mask": "mapInt32Int32"})",
      .expected = R"(
        {"mapInt32Int32": {"1":6,"2":8,"3":9}, "optionalString":"foo"}
      )",
    },
    merge_test_case{
      .name = "NestedMessageFullUpdate",
      .input = R"(
        {"optionalNestedMessage":{"a":42,"corecursive":{"optionalBool":true}}}
       )",
      .update = R"(
        {"optionalNestedMessage":{"a":43,"corecursive":{"optionalString":"bar"}}}
       )",
      .mask = R"({"mask": "optionalNestedMessage"})",
      .expected = R"(
        {"optionalNestedMessage":{"a":43,"corecursive":{"optionalString":"bar"}}}
      )",
    },
    merge_test_case{
      .name = "NestedMessagePartialUpdate",
      .input = R"(
        {"optionalNestedMessage":{"a":42,"corecursive":{"optionalBool":true}}}
       )",
      .update = R"(
        {"optionalNestedMessage":{"a":43,"corecursive":{"optionalString":"bar"}}}
       )",
      .mask = R"({"mask": "optionalNestedMessage.a"})",
      .expected = R"(
        {"optionalNestedMessage":{"a":43,"corecursive":{"optionalBool":true}}}
      )",
    },
    merge_test_case{
      .name = "DeepNestedMessagePartialUpdate",
      .input = R"(
        {"optionalNestedMessage":{"a":42,"corecursive":{"optionalBool":true}}}
       )",
      .update = R"(
        {"optionalNestedMessage":{"a":43,"corecursive":{"optionalString":"bar"}}}
       )",
      .mask = R"({"mask": "optionalNestedMessage.corecursive.optionalString"})",
      .expected = R"(
        {"optionalNestedMessage":{"a":42,"corecursive":{"optionalBool":true,"optionalString":"bar"}}}
      )",
    },
    merge_test_case{
      .name = "NestedPtrMessage",
      .input = R"({"recursiveMessage":{"optionalInt32": 1}})",
      .update = R"(
         {"optionalInt32": 3, "recursiveMessage":{"optionalBool":true}}
       )",
      .mask = R"({"mask": "recursiveMessage.optionalBool,optionalInt32"})",
      .expected = R"(
        {"optionalInt32": 3, "recursiveMessage":{"optionalInt32":1,"optionalBool":true}}
      )",
    },
    merge_test_case{
      .name = "NestedPtrMessageNullNestedSet",
      .input = R"({"recursiveMessage":{"optionalInt32": 1}})",
      .update = R"(
         {"optionalInt32": 3, "recursiveMessage":null}
       )",
      .mask = R"({"mask": "recursiveMessage.optionalInt32"})",
      .expected = R"(
        {"recursiveMessage":{}}
      )",
    },
    merge_test_case{
      .name = "NestedPtrMessageDefault",
      .input = R"({"recursiveMessage":{"optionalInt32": 1}})",
      .update = R"(
         {"optionalInt32": 3, "recursiveMessage":{}}
       )",
      .mask = R"({"mask": "recursiveMessage,optionalInt32"})",
      .expected = R"(
        {"optionalInt32": 3, "recursiveMessage":{}}
      )",
    },
    merge_test_case{
      .name = "NestedPtrMessageNull",
      .input = R"({"recursiveMessage":{"optionalInt32": 1}})",
      .update = R"(
         {"recursiveMessage":null}
       )",
      .mask = R"({"mask": "recursiveMessage"})",
      .expected = R"(
        {"recursiveMessage":null}
      )",
    },
    merge_test_case{
      .name = "OneofMatch",
      .input = R"({"oneofUint32": 1})",
      .update = R"({"oneofUint32": 3})",
      .mask = R"({"mask": "oneofUint32"})",
      .expected = R"({"oneofUint32": 3})",
    },
    merge_test_case{
      .name = "OneofMismatch",
      .input = R"({"oneofUint32": 1})",
      .update = R"({"oneofBool": true})",
      .mask = R"({"mask": "oneofBool"})",
      .expected = R"({"oneofBool": true})",
    },
    merge_test_case{
      .name = "OneofMismatchFromMessage",
      .input = R"({"oneofNestedMessage": {"a":4}})",
      .update = R"({"oneofBool": true})",
      .mask = R"({"mask": "oneofBool"})",
      .expected = R"({"oneofBool": true})",
    },
    merge_test_case{
      .name = "OneofMismatchToMessage",
      .input = R"({"oneofBool": true})",
      .update = R"({"oneofNestedMessage": {"a":5}})",
      .mask = R"({"mask": "oneofNestedMessage"})",
      .expected = R"({"oneofNestedMessage": {"a":5}})",
    },
    merge_test_case{
      .name = "OneofMismatchToMessageField",
      .input = R"({"oneofBool": true})",
      .update = R"({"oneofNestedMessage": {"a":5}})",
      .mask = R"({"mask": "oneofNestedMessage.a"})",
      .expected = R"({"oneofNestedMessage": {"a":5}})",
    },
    merge_test_case{
      .name = "OneofMismatchUnset",
      .input = R"({"oneofUint32": 1})",
      .update = R"({"oneofBool": true})",
      .mask = R"({"mask": "oneofUint32"})",
      .expected = R"({})",
    }));
