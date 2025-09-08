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

#include "base/format_to.h"
#include "lsm/core/internal/keys.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/weak_intrusive_list.h"
#include "lsm/io/persistence.h"

namespace lsm::db {

class version_set;

// A single immutable version of the database.
class version : public weak_intrusive_list<version> {
public:
    version() = delete;
    version(const version&) = delete;
    version(version&&) = delete;
    version& operator=(const version&) = delete;
    version& operator=(version&&) = delete;
    ~version() = default;

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class version_set;
    friend class compaction;
    explicit version(version_set* vset)
      : _vset(vset) {}

    version_set* _vset; // the set which this version belongs to
    // All the files in this version of the database.
    absl::flat_hash_map<
      internal::level,
      chunked_vector<ss::lw_shared_ptr<file_meta_data>>>
      _files;

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
public:
    // Return the current version of this set.
    ss::lw_shared_ptr<version> current() { return _current; }

private:
    void set_current(ss::lw_shared_ptr<version>);

    ss::lw_shared_ptr<version> _current;
    io::persistence* _persistence;
};

// Encapulate information about a compaction event.
class compaction {
public:
private:
};

} // namespace lsm::db
