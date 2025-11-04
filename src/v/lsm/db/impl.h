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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "lsm/core/internal/batch.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/memtable.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"
#include "ssx/condition_variable.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm::db {

// The implementation of the database.
class impl {
    // The impl constructor is private.
    struct ctor {};

public:
    explicit impl(
      ctor,
      std::unique_ptr<io::persistence>,
      ss::lw_shared_ptr<internal::options>);
    impl(impl&&) = delete;
    impl(const impl&) = delete;
    impl& operator=(impl&&) = delete;
    impl& operator=(const impl&) = delete;
    ~impl() = default;

    // the maximum sequence number that has been applied to in memory state or
    // durable storage.
    internal::sequence_number max_applied_seqno() const;

    // The maximum sequence number that has been persisted to durable storage.
    internal::sequence_number max_persisted_seqno() const;

    // Open the database
    static ss::future<std::unique_ptr<impl>> open(
      ss::lw_shared_ptr<internal::options>, std::unique_ptr<io::persistence>);

    // Apply a batch of writes to the database atomically.
    ss::future<> apply(internal::write_batch);

    // Get a key from the database
    ss::future<lookup_result> get(internal::key_view);

    // Create an iterator over the database. Note that this iterator
    // results in ALL entries from the database, a deduplicating iterator
    // needs to be added on top to give a traditional iterator view.
    ss::future<std::unique_ptr<internal::iterator>> create_iterator();

    // Flush any pending state in memtables to disk.
    ss::future<> flush();

    // Close the database, no more operations should happen to the database at
    // this point.
    //
    // This *must* be called before destroying the database.
    ss::future<> close();

private:
    ss::future<std::unique_ptr<internal::iterator>> create_internal_iterator();

    ss::future<> recover();

    ss::future<> make_room_for_write();

    void maybe_schedule_compaction();

    ss::future<> run_background_compaction();

    ss::future<> flush_memtable();

    ss::future<> remove_obsolete_files();

    std::unique_ptr<io::persistence> _persistence;
    ss::lw_shared_ptr<internal::options> _opts;
    // The active in-memory memtable.
    ss::lw_shared_ptr<memtable> _mem;
    // The memtable being compacted
    ss::optimized_optional<ss::lw_shared_ptr<memtable>> _imm;
    std::unique_ptr<table_cache> _table_cache;
    std::unique_ptr<version_set> _versions;
    ssx::condition_variable _start_background_work_signal;
    ssx::condition_variable _background_work_finished_signal;
    ss::abort_source _as;
    bool _background_work_running = false;
    std::optional<ss::future<>> _background_work;
};

} // namespace lsm::db
