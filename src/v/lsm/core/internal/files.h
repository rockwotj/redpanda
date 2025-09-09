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
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace lsm::internal {

// The level in the LSM tree.
using level = named_type<uint8_t, struct level_tag>;

// The numeric ID of an sst file
using file_id = named_type<uint64_t, struct file_id_tag>;

// Compute the name of an sst file with the given ID.
ss::sstring sst_file_name(file_id);

} // namespace lsm::internal
