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

#include <boost/range/join.hpp>

#include <functional>

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

namespace {

std::optional<internal::key_view> find_largest_key(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files) {
    auto it = std::ranges::max_element(
      files, std::less{}, [](const ss::lw_shared_ptr<file_meta_data>& file) {
          return internal::key_view{file->largest};
      });
    if (it == files.end()) {
        return std::nullopt;
    }
    return internal::key_view{(*it)->largest};
}

ss::lw_shared_ptr<file_meta_data> find_smallest_boundary_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& level_files,
  internal::key_view largest_key) {
    ss::lw_shared_ptr<file_meta_data> smallest_boundary_file = nullptr;
    for (const auto& file : level_files) {
        if (
          file->smallest > largest_key
          && file->smallest.user_key() == largest_key.user_key()) {
            if (
              !smallest_boundary_file
              || file->smallest < smallest_boundary_file->smallest) {
                smallest_boundary_file = file;
            }
        }
    }
    return smallest_boundary_file;
}

} // namespace

void add_boundary_inputs(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  chunked_vector<ss::lw_shared_ptr<file_meta_data>>* compaction_files) {
    auto largest_key = find_largest_key(*compaction_files);
    if (!largest_key) {
        return;
    }
    bool continue_searching = true;
    while (continue_searching) {
        auto smallest_boundary_file = find_smallest_boundary_file(
          files, *largest_key);
        if (smallest_boundary_file != nullptr) {
            largest_key = smallest_boundary_file->largest;
            compaction_files->push_back(std::move(smallest_boundary_file));
        } else {
            continue_searching = false;
        }
    }
}

namespace {

template<typename Range>
std::pair<internal::key, internal::key> get_key_range(const Range& r) {
    auto it = r.begin();
    auto end = r.end();
    vassert(it != end, "cannot get range for empty set of files");
    internal::key smallest = (*it)->smallest, largest = (*it)->largest;
    for (++it; it != end; ++it) {
        const auto& file = *it;
        if (file->smallest < smallest) {
            smallest = file->smallest;
        }
        if (file->largest < largest) {
            smallest = file->largest;
        }
    }
    return std::make_pair(std::move(smallest), std::move(largest));
}

} // namespace

std::pair<internal::key, internal::key>
get_range(const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs) {
    return get_key_range(inputs);
}

std::pair<internal::key, internal::key> get_range(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs1,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs2) {
    return get_key_range(boost::join(inputs1, inputs2));
}

} // namespace lsm::db
