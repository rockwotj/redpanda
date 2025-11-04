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

#include "absl/container/fixed_array.h"
#include "base/format_to.h"
#include "container/chunked_hash_map.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"

#include <seastar/core/shared_ptr.hh>

#include <cstdint>

namespace lsm::db {

// The default number of seeks before compaction is triggered.
constexpr static int32_t default_allowed_seeks = 1024 * 1024 * 1024;

// All the metadata for a single SST file.
struct file_meta_data {
    // The file's numeric ID.
    internal::file_id id;
    // Size of the file in bytes
    uint64_t file_size = 0;
    internal::key smallest; // smallest key in the table
    internal::key largest;  // largest key in the table
    internal::sequence_number oldest_seqno;
    internal::sequence_number newest_seqno;
    // Allowed seeks before compaction
    int32_t allowed_seeks = default_allowed_seeks;

    bool operator==(const file_meta_data&) const = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

// A class representing all the incremental changes needed to progress from one
// version to another version.
class version_edit {
public:
    explicit version_edit(const internal::options& options)
      : _mutations_by_level(options.levels.size()) {}

    // Set the compaction pointer, which is where the next compaction should
    // begin.
    void set_compact_pointer(internal::level level, internal::key key) {
        _mutations_by_level[level].compact_pointer = std::move(key);
    }

    // Set the latest seqno for the data written, this only needs to be set when
    // new data is added to the database which is only memtable flushes.
    void set_last_seqno(internal::sequence_number seqno) {
        _last_seqno = seqno;
    }

    // The parameters to `add_file`
    struct added_file {
        internal::level level;
        internal::file_id file_id;
        uint64_t file_size;
        internal::key smallest;
        internal::key largest;
        internal::sequence_number oldest_seqno;
        internal::sequence_number newest_seqno;
    };

    // Add a file to the new version
    void add_file(added_file params) {
        _mutations_by_level[params.level].added_files.push_back(
          ss::make_lw_shared<file_meta_data>({
            .id = params.file_id,
            .file_size = params.file_size,
            .smallest = std::move(params.smallest),
            .largest = std::move(params.largest),
            .oldest_seqno = params.oldest_seqno,
            .newest_seqno = params.newest_seqno,
          }));
    }

    // Remove a file from this version.
    void remove_file(internal::level level, internal::file_id file_id) {
        _mutations_by_level[level].removed_files.insert(file_id);
    }

    fmt::iterator format_to(fmt::iterator it) const;

private:
    friend class version_set;
    struct mutation {
        chunked_hash_set<internal::file_id> removed_files;
        chunked_vector<ss::lw_shared_ptr<file_meta_data>> added_files;
        std::optional<internal::key> compact_pointer;

        fmt::iterator format_to(fmt::iterator) const;
    };
    absl::FixedArray<mutation> _mutations_by_level;
    // This is safe because it is applied idempotently.
    internal::sequence_number _last_seqno = internal::sequence_number::min();
};

} // namespace lsm::db
