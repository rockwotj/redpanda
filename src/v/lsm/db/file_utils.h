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

#pragma once

#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/version_edit.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::db {

struct by_smallest_key {
    using is_transparent = void;

    bool operator()(
      const ss::lw_shared_ptr<file_meta_data>& a,
      const ss::lw_shared_ptr<file_meta_data>& b) const {
        // Sort by smallest key, and break ties using file ID.
        return std::tie(a->smallest, a->id) < std::tie(b->smallest, b->id);
    }
};

size_t
total_file_size(const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files);

size_t max_bytes_for_level(internal::level level);

inline size_t target_file_size(const internal::options& options) {
    return options.max_file_size;
}

inline size_t max_grandparent_overlap_bytes(const internal::options& options) {
    return 10 * target_file_size(options);
}

inline size_t
expanded_compaction_byte_size_limit(const internal::options& options) {
    return 25 * target_file_size(options);
}

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view target);

// Returns true iff some file in `files` overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key larger than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges in
// sorted order.
bool some_file_overlaps_range(
  bool disjoint_sorted_files,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view* smallest_key,
  internal::key_view* largest_key);

// Extracts the largest file b1 from `compaction_files` and then searches for a
// b2 in `level_files` for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void add_boundary_inputs(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  chunked_vector<ss::lw_shared_ptr<file_meta_data>>* compaction_files);

} // namespace lsm::db
