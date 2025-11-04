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
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/io/persistence.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm::db {

// The result of building an SST table. It includes the size of the file as well
// as metadata about the bounds of data written.
struct build_table_result {
    uint64_t file_size;
    internal::key smallest;
    internal::key largest;
    internal::sequence_number oldest_seqno;
    internal::sequence_number newest_seqno;
};

// Create an SST file with the given ID from the provided iterator.
ss::future<std::optional<build_table_result>> build_table(
  io::persistence* persistence,
  internal::file_id id,
  std::unique_ptr<internal::iterator> iter,
  ss::lw_shared_ptr<internal::options> opts,
  ss::abort_source*);

} // namespace lsm::db
