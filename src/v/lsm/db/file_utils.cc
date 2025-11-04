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

#include "lsm/db/file_utils.h"

namespace lsm::db {

using internal::operator""_level;

size_t max_bytes_for_level(internal::level level) {
    // NOLINTBEGIN(*magic-number*)
    size_t result = 10_MiB;
    while (level > 1_level) {
        result *= 10;
        --level;
    }
    // NOLINTEND(*magic-number*)
    return result;
}
size_t total_file_size(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files) {
    size_t total = 0;
    for (const auto& file : files) {
        total += file->file_size;
    }
    return total;
}
size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view target) {
    size_t left = 0;
    size_t right = files.size();
    while (left < right) {
        size_t mid = (left + right) / 2;
        const auto& f = files[mid];
        if (f->largest < target) {
            // kkey at mid.largest is < target. Therefore all files at or before
            // mid are uninteresting.
            left = mid + 1;
        } else {
            // Key at mid.largest is >= target. Therefore all files after "mid"
            // are uninteresting.
            right = mid;
        }
    }
    return right;
}

namespace {

bool after_file(
  const file_meta_data& file, const std::optional<internal::key_view>& key) {
    return key && *key > file.largest;
}

bool before_file(
  const file_meta_data& file, const std::optional<internal::key_view>& key) {
    return key && *key < file.smallest;
}

} // namespace

bool some_file_overlaps_range(
  bool disjoint_sorted_files,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  std::optional<internal::key_view> smallest_key,
  std::optional<internal::key_view> largest_key) {
    if (!disjoint_sorted_files) {
        // Need to check against all files
        for (const auto& file : files) {
            if (
              after_file(*file, smallest_key)
              || before_file(*file, largest_key)) {
                // no overlap
            } else {
                return true; // overlap
            }
        }
        return false;
    }
    size_t index = 0;
    if (smallest_key) {
        index = find_file(files, *smallest_key);
    }
    if (index >= files.size()) {
        // beginning of range is after all files, so no overlap.
        return false;
    }
    return !before_file(*files[index], largest_key);
}

} // namespace lsm::db
