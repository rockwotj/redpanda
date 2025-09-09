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

#include "lsm/core/internal/files.h"

#include <seastar/core/format.hh>

namespace lsm::internal {

ss::sstring sst_file_name(file_id id) { return ss::format("{}.sst", id()); }

} // namespace lsm::internal
