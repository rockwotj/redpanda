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

#include <gtest/gtest.h>

#include <limits>

namespace {
using namespace lsm::internal;
}

TEST(Files, SstFileName) {
    EXPECT_EQ("00000000000000000001.sst", sst_file_name(file_id{1}));
    EXPECT_EQ("00000000000000000123.sst", sst_file_name(file_id{123}));
    EXPECT_EQ("00000000000000000000.sst", sst_file_name(file_id{0}));
    EXPECT_EQ(
      "18446744073709551615.sst",
      sst_file_name(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, ManifestFileName) {
    EXPECT_EQ("00000000000000000001.manifest", manifest_file_name(file_id{1}));
    EXPECT_EQ(
      "00000000000000000123.manifest", manifest_file_name(file_id{123}));
    EXPECT_EQ("00000000000000000000.manifest", manifest_file_name(file_id{0}));
    EXPECT_EQ(
      "18446744073709551615.manifest",
      manifest_file_name(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, CurrentFileName) { EXPECT_EQ("CURRENT", current_file_name()); }

