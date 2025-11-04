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

// TODO(lsm): This should be a config knob probably
size_t max_bytes_for_level(internal::level level);

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view target);

// Returns true iff some file in `files` overlaps the user key range
// [*smallest,*largest].
// smallest==nullopt represents a key smaller than all keys in the DB.
// largest==nullopt represents a key larger than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges in
// sorted order.
bool some_file_overlaps_range(
  bool disjoint_sorted_files,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  std::optional<internal::key_view> smallest_key,
  std::optional<internal::key_view> largest_key);

} // namespace lsm::db
