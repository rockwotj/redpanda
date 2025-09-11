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
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/weak_intrusive_list.h"
#include "lsm/io/persistence.h"

namespace lsm::db {

class version_set;

// A single immutable version of the database.
class version : public weak_intrusive_list<version> {
    // A private struct so that the constructor can only be used
    // internal to version (or friended classes).
    struct ctor {};

public:
    struct get_stats {
        ss::optimized_optional<ss::lw_shared_ptr<file_meta_data>> seek_file;
        internal::level seek_file_level;
    };

    version(ctor, version_set* vset);

    version() = delete;
    version(const version&) = delete;
    version(version&&) = delete;
    version& operator=(const version&) = delete;
    version& operator=(version&&) = delete;
    ~version() = default;

    // Append to *iters a sequence of iterators that will yield the contents of
    // this version when merged together.
    // REQUIRES: This version has been applied to a version_set.
    void add_iterators(chunked_vector<internal::iterator>* iters);

    // Adds stats into the current state. Returns true if a new compaction may
    // need to be trigged, false otherwise.
    bool update_stats(const get_stats& stats);

    // Record a sample of bytes read at the specified internal key.
    // samples are take approximately once every read_bytes_period bytes.
    // Returns true if a new compaction may need to be trigged.
    bool record_read_sample(internal::key_view);

    // Return all files in level that overlap [begin,end].
    //
    // begin==nullptr, means before all keys.
    // end==nullptr, means after all keys.
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> get_overlapping_inputs(
      internal::level,
      const internal::key_view* begin,
      const internal::key_view* end);

    // Lookup the value for key.
    ss::future<std::optional<iobuf>> get(internal::key_view);

    // Returns true if some file in the specified level overlaps some part of
    // the specified key range.
    //
    // begin==nullptr, means before all keys.
    // end==nullptr, means after all keys.
    bool overlap_in_level(
      internal::level,
      const internal::key_view* begin,
      const internal::key_view* end);

    // Return the level at which we should place a new memtable compaction
    // result that covers the range [begin,end].
    internal::level pick_level_for_memtable_output(
      internal::key_view begin, internal::key_view end);

    size_t num_files(internal::level level) { return _files[level].size(); }

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class version_set;
    friend class compaction;

    std::unique_ptr<internal::iterator>
      create_concatenating_iterator(internal::level);

    version_set* _vset; // the set which this version belongs to
    // All the files in this version of the database.
    absl::FixedArray<chunked_vector<ss::lw_shared_ptr<file_meta_data>>> _files;

    // The next file to compact based on seek stats.
    ss::optimized_optional<ss::lw_shared_ptr<file_meta_data>> _file_to_compact;
    internal::level _file_to_compact_level;

    // The level that should be compacted next and it's compaction score.
    // Score < 1 means that compaction is not strictly needed.
    double _compaction_score = 0;
    internal::level _compaction_level;
};

// The representation of a database is a set of versions. The newest version is
// called "current". Older versions may be kept around to get a consistent view
// to live iterators.
//
// Each version keeps track of a set of table files per level. The entire set of
// versions is maintained in this data structure.
class version_set {
    class builder;

public:
    version_set(
      io::persistence* persistence,
      table_cache* table_cache,
      internal::options options);

    // Return the current version of this set.
    ss::lw_shared_ptr<version> current() { return _current; }

    // Apply the edit to form a new version of the database that is both saved
    // to persistence as well as set to be the current version.
    ss::future<> log_and_apply(version_edit);

private:
    friend class version;

    void set_current(ss::lw_shared_ptr<version>);
    void finalize(version*);
    // Write this version to a manifest file as a snapshot.
    ss::future<> write_manifest(version*, io::sequential_file_writer*);

    io::persistence* _persistence;
    table_cache* _table_cache;
    internal::options _options;
    ss::lw_shared_ptr<version> _current;
    internal::file_id _next_file_id = internal::file_id{2};
    internal::manifest_id _manifest_id;
    internal::seqno _last_seqno;
    absl::FixedArray<std::optional<internal::key>> _compact_pointer;
};

// Encapulate information about a compaction event.
class compaction {
public:
private:
};

} // namespace lsm::db
