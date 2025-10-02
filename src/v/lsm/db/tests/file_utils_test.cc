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
#include "lsm/core/internal/keys.h"
#include "lsm/db/file_utils.h"
#include "lsm/db/version_edit.h"

#include <seastar/core/shared_ptr.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

using namespace lsm;
using lsm::internal::operator""_seqno;
using ::testing::ElementsAre;

constexpr static auto default_seqno = internal::sequence_number{100};

class FindFileTest : public testing::Test {
protected:
    void add(
      const std::string& smallest,
      const std::string& largest,
      internal::sequence_number smallest_seq = default_seqno,
      internal::sequence_number largest_seq = default_seqno) {
        _files.push_back(
          ss::make_lw_shared<db::file_meta_data>(db::file_meta_data{
            .id = internal::file_id{static_cast<uint64_t>(_files.size())},
            .file_size = 100,
            .smallest = internal::key::encode(
              {.key = smallest, .seqno = smallest_seq}),
            .largest = internal::key::encode(
              {.key = largest, .seqno = largest_seq}),
          }));
    }

    size_t find(const std::string& key) {
        auto encoded = internal::key::encode({
          .key = key,
          .seqno = default_seqno,
        });
        return db::find_file(_files, encoded);
    }

    bool overlaps_level0(const char* smallest, const char* largest) {
        return overlaps(false, smallest, largest);
    }
    bool overlaps_disjoint(const char* smallest, const char* largest) {
        return overlaps(true, smallest, largest);
    }

private:
    bool overlaps(bool disjoint, const char* smallest, const char* largest) {
        internal::key s, l;
        if (smallest != nullptr) {
            s = internal::key::encode({
              .key = smallest,
              .seqno = default_seqno,
            });
        }
        if (largest != nullptr) {
            l = internal::key::encode({
              .key = largest,
              .seqno = default_seqno,
            });
        }
        return db::some_file_overlaps_range(
          disjoint,
          _files,
          smallest ? std::make_optional(s) : std::nullopt,
          largest ? std::make_optional(l) : std::nullopt);
    }
    chunked_vector<ss::lw_shared_ptr<db::file_meta_data>> _files;
};

TEST_F(FindFileTest, Empty) {
    ASSERT_EQ(0, find("foo"));
    ASSERT_TRUE(!overlaps_disjoint("a", "z"));
    ASSERT_TRUE(!overlaps_disjoint(nullptr, "a"));
    ASSERT_TRUE(!overlaps_disjoint("a", nullptr));
    ASSERT_TRUE(!overlaps_disjoint(nullptr, nullptr));
}

TEST_F(FindFileTest, Single) {
    add("p", "q");

    ASSERT_EQ(0, find("a"));
    ASSERT_EQ(0, find("p"));
    ASSERT_EQ(0, find("p1"));
    ASSERT_EQ(0, find("q"));
    ASSERT_EQ(1, find("q1"));
    ASSERT_EQ(1, find("z"));

    ASSERT_FALSE(overlaps_disjoint("a", "b"));
    ASSERT_FALSE(overlaps_disjoint("z1", "z2"));
    ASSERT_TRUE(overlaps_disjoint("a", "p"));
    ASSERT_TRUE(overlaps_disjoint("a", "q"));
    ASSERT_TRUE(overlaps_disjoint("a", "z"));
    ASSERT_TRUE(overlaps_disjoint("p", "p1"));
    ASSERT_TRUE(overlaps_disjoint("p", "q"));
    ASSERT_TRUE(overlaps_disjoint("p1", "p2"));
    ASSERT_TRUE(overlaps_disjoint("p1", "z"));
    ASSERT_TRUE(overlaps_disjoint("q", "q"));
    ASSERT_TRUE(overlaps_disjoint("q", "q1"));

    ASSERT_FALSE(overlaps_disjoint(nullptr, "j"));
    ASSERT_FALSE(overlaps_disjoint("r", nullptr));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "p"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "p1"));
    ASSERT_TRUE(overlaps_disjoint("q", nullptr));
    ASSERT_TRUE(overlaps_disjoint(nullptr, nullptr));
}

TEST_F(FindFileTest, Multiple) {
    add("150", "200");
    add("200", "250");
    add("300", "350");
    add("400", "450");

    ASSERT_EQ(0, find("100"));
    ASSERT_EQ(0, find("150"));
    ASSERT_EQ(0, find("151"));
    ASSERT_EQ(0, find("199"));
    ASSERT_EQ(0, find("200"));
    ASSERT_EQ(1, find("201"));
    ASSERT_EQ(1, find("249"));
    ASSERT_EQ(1, find("250"));
    ASSERT_EQ(2, find("251"));
    ASSERT_EQ(2, find("299"));
    ASSERT_EQ(2, find("300"));
    ASSERT_EQ(2, find("349"));
    ASSERT_EQ(2, find("350"));
    ASSERT_EQ(3, find("351"));
    ASSERT_EQ(3, find("400"));
    ASSERT_EQ(3, find("450"));
    ASSERT_EQ(4, find("451"));

    ASSERT_FALSE(overlaps_disjoint("100", "149"));
    ASSERT_FALSE(overlaps_disjoint("251", "299"));
    ASSERT_FALSE(overlaps_disjoint("451", "500"));
    ASSERT_FALSE(overlaps_disjoint("351", "399"));

    ASSERT_TRUE(overlaps_disjoint("100", "150"));
    ASSERT_TRUE(overlaps_disjoint("100", "200"));
    ASSERT_TRUE(overlaps_disjoint("100", "300"));
    ASSERT_TRUE(overlaps_disjoint("100", "400"));
    ASSERT_TRUE(overlaps_disjoint("100", "500"));
    ASSERT_TRUE(overlaps_disjoint("375", "400"));
    ASSERT_TRUE(overlaps_disjoint("450", "450"));
    ASSERT_TRUE(overlaps_disjoint("450", "500"));
}

TEST_F(FindFileTest, MultipleNullBoundaries) {
    add("150", "200");
    add("200", "250");
    add("300", "350");
    add("400", "450");

    ASSERT_FALSE(overlaps_disjoint(nullptr, "149"));
    ASSERT_FALSE(overlaps_disjoint("451", nullptr));
    ASSERT_TRUE(overlaps_disjoint(nullptr, nullptr));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "150"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "199"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "200"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "201"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "400"));
    ASSERT_TRUE(overlaps_disjoint(nullptr, "800"));
    ASSERT_TRUE(overlaps_disjoint("100", nullptr));
    ASSERT_TRUE(overlaps_disjoint("200", nullptr));
    ASSERT_TRUE(overlaps_disjoint("449", nullptr));
    ASSERT_TRUE(overlaps_disjoint("450", nullptr));
}

TEST_F(FindFileTest, OverlappingFiles) {
    add("150", "600");
    add("400", "500");

    ASSERT_FALSE(overlaps_level0("100", "149"));
    ASSERT_FALSE(overlaps_level0("601", "700"));
    ASSERT_TRUE(overlaps_level0("100", "150"));
    ASSERT_TRUE(overlaps_level0("100", "200"));
    ASSERT_TRUE(overlaps_level0("100", "300"));
    ASSERT_TRUE(overlaps_level0("100", "400"));
    ASSERT_TRUE(overlaps_level0("100", "500"));
    ASSERT_TRUE(overlaps_level0("375", "400"));
    ASSERT_TRUE(overlaps_level0("450", "450"));
    ASSERT_TRUE(overlaps_level0("450", "500"));
    ASSERT_TRUE(overlaps_level0("450", "700"));
    ASSERT_TRUE(overlaps_level0("600", "700"));
}

class AddBoundaryInputsTest : public testing::Test {
public:
    ss::lw_shared_ptr<db::file_meta_data> create_file(
      uint64_t id,
      internal::key::parts smallest,
      internal::key::parts largest) {
        auto meta_data = ss::make_lw_shared<db::file_meta_data>();
        meta_data->id = internal::file_id{id};
        meta_data->file_size = 100;
        meta_data->smallest = internal::key::encode(std::move(smallest));
        meta_data->largest = internal::key::encode(std::move(largest));
        return meta_data;
    }

protected:
    chunked_vector<ss::lw_shared_ptr<db::file_meta_data>> level_files;
    chunked_vector<ss::lw_shared_ptr<db::file_meta_data>> compaction_files;
};

TEST_F(AddBoundaryInputsTest, TestEmptyFileSets) {
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_TRUE(level_files.empty());
    ASSERT_TRUE(compaction_files.empty());
}

TEST_F(AddBoundaryInputsTest, TestEmptyLevelFiles) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 2_seqno}, {.key = "100", .seqno = 1_seqno});
    compaction_files.push_back(f1);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_TRUE(level_files.empty());
    ASSERT_THAT(compaction_files, ElementsAre(f1));
}

TEST_F(AddBoundaryInputsTest, TestEmptyCompactionFiles) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 2_seqno}, {.key = "100", .seqno = 1_seqno});
    level_files.push_back(f1);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_THAT(level_files, ElementsAre(f1));
    ASSERT_TRUE(compaction_files.empty());
}

TEST_F(AddBoundaryInputsTest, TestNoBoundaryFiles) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 2_seqno}, {.key = "100", .seqno = 1_seqno});
    auto f2 = create_file(
      2, {.key = "200", .seqno = 2_seqno}, {.key = "200", .seqno = 1_seqno});
    auto f3 = create_file(
      3, {.key = "300", .seqno = 2_seqno}, {.key = "300", .seqno = 1_seqno});
    level_files.push_back(f3);
    level_files.push_back(f2);
    level_files.push_back(f1);
    compaction_files.push_back(f2);
    compaction_files.push_back(f3);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_THAT(compaction_files, ElementsAre(f2, f3));
}

TEST_F(AddBoundaryInputsTest, TestOneBoundaryFile) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 3_seqno}, {.key = "100", .seqno = 2_seqno});
    auto f2 = create_file(
      2, {.key = "100", .seqno = 1_seqno}, {.key = "200", .seqno = 3_seqno});
    auto f3 = create_file(
      3, {.key = "300", .seqno = 2_seqno}, {.key = "300", .seqno = 1_seqno});
    level_files.push_back(f3);
    level_files.push_back(f2);
    level_files.push_back(f1);
    compaction_files.push_back(f1);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_THAT(compaction_files, ElementsAre(f1, f2));
}

TEST_F(AddBoundaryInputsTest, TestTwoBoundaryFiles) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 6_seqno}, {.key = "100", .seqno = 5_seqno});
    auto f2 = create_file(
      2, {.key = "100", .seqno = 2_seqno}, {.key = "300", .seqno = 1_seqno});
    auto f3 = create_file(
      3, {.key = "100", .seqno = 4_seqno}, {.key = "100", .seqno = 3_seqno});
    level_files.push_back(f2);
    level_files.push_back(f3);
    level_files.push_back(f1);
    compaction_files.push_back(f1);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_THAT(compaction_files, ElementsAre(f1, f3, f2));
}

TEST_F(AddBoundaryInputsTest, TestDisjointFilePointers) {
    auto f1 = create_file(
      1, {.key = "100", .seqno = 6_seqno}, {.key = "100", .seqno = 5_seqno});
    auto f2 = create_file(
      2, {.key = "100", .seqno = 6_seqno}, {.key = "300", .seqno = 5_seqno});
    auto f3 = create_file(
      3, {.key = "100", .seqno = 2_seqno}, {.key = "100", .seqno = 1_seqno});
    level_files.push_back(f2);
    auto f4 = create_file(
      4, {.key = "100", .seqno = 4_seqno}, {.key = "100", .seqno = 3_seqno});
    level_files.push_back(f2);
    level_files.push_back(f3);
    level_files.push_back(f4);
    compaction_files.push_back(f1);
    db::add_boundary_inputs(level_files, &compaction_files);
    ASSERT_THAT(compaction_files, ElementsAre(f1, f4, f3));
}

} // namespace
