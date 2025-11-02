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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

namespace {
using namespace lsm::internal;
using ::testing::AllOf;
using ::testing::Eq;
using ::testing::Field;
using ::testing::Optional;
} // namespace

TEST(Files, SstFileName) {
    EXPECT_EQ("00000000000000000001.sst", sst_file_name(1_file_id));
    EXPECT_EQ("00000000000000000123.sst", sst_file_name(123_file_id));
    EXPECT_EQ("00000000000000000000.sst", sst_file_name(0_file_id));
    EXPECT_EQ(
      "18446744073709551615.sst",
      sst_file_name(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, ManifestFileName) {
    EXPECT_EQ("00000000000000000001.manifest", manifest_file_name(1_file_id));
    EXPECT_EQ("00000000000000000123.manifest", manifest_file_name(123_file_id));
    EXPECT_EQ("00000000000000000000.manifest", manifest_file_name(0_file_id));
    EXPECT_EQ(
      "18446744073709551615.manifest",
      manifest_file_name(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, CurrentFileName) { EXPECT_EQ("CURRENT", current_file_name()); }

TEST(Files, ParseFilename_Current) {
    EXPECT_THAT(
      parse_filename(current_file_name()),
      Optional(Field(&parsed_filename::type, Eq(file_type::current))));
}

TEST(Files, ParseFilename_SstFile) {
    EXPECT_THAT(
      parse_filename(sst_file_name(1_file_id)),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::sst)),
        Field(&parsed_filename::id, Eq(1_file_id)))));

    EXPECT_THAT(
      parse_filename(sst_file_name(123_file_id)),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::sst)),
        Field(&parsed_filename::id, Eq(123_file_id)))));

    EXPECT_THAT(
      parse_filename(
        sst_file_name(file_id{std::numeric_limits<uint64_t>::max()})),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::sst)),
        Field(
          &parsed_filename::id,
          Eq(file_id{std::numeric_limits<uint64_t>::max()})))));
}

TEST(Files, ParseFilename_ManifestFile) {
    EXPECT_THAT(
      parse_filename(manifest_file_name(1_file_id)),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::manifest)),
        Field(&parsed_filename::id, Eq(1_file_id)))));

    EXPECT_THAT(
      parse_filename(manifest_file_name(123_file_id)),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::manifest)),
        Field(&parsed_filename::id, Eq(123_file_id)))));

    EXPECT_THAT(
      parse_filename(
        manifest_file_name(file_id{std::numeric_limits<uint64_t>::max()})),
      Optional(AllOf(
        Field(&parsed_filename::type, Eq(file_type::manifest)),
        Field(
          &parsed_filename::id,
          Eq(file_id{std::numeric_limits<uint64_t>::max()})))));
}

TEST(Files, ParseFilename_TmpFile) {
    EXPECT_THAT(
      parse_filename("somefile.lsm-staging"),
      Optional(Field(&parsed_filename::type, Eq(file_type::tmp))));

    EXPECT_THAT(
      parse_filename("0.lsm-staging"),
      Optional(Field(&parsed_filename::type, Eq(file_type::tmp))));
}

TEST(Files, ParseFilename_Invalid) {
    // No extension
    EXPECT_EQ(std::nullopt, parse_filename("noextension"));

    // Unknown extension
    EXPECT_EQ(std::nullopt, parse_filename("file.txt"));
    EXPECT_EQ(std::nullopt, parse_filename("00000000000000000001.log"));

    // Invalid numeric ID for sst
    EXPECT_EQ(std::nullopt, parse_filename("notanumber.sst"));
    EXPECT_EQ(std::nullopt, parse_filename("abc123.sst"));

    // Invalid numeric ID for manifest
    EXPECT_EQ(std::nullopt, parse_filename("notanumber.manifest"));
    EXPECT_EQ(std::nullopt, parse_filename("abc123.manifest"));

    // Empty string
    EXPECT_EQ(std::nullopt, parse_filename(""));

    // Just extension
    EXPECT_EQ(std::nullopt, parse_filename(".sst"));
    EXPECT_EQ(std::nullopt, parse_filename(".manifest"));
}
