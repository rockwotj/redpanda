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

#include "lsm/io/persistence.h"

#include <filesystem>

namespace lsm::io {

// Open a persistence object at the specified directory.
ss::future<std::unique_ptr<persistence>>
open_disk_persistence(std::filesystem::path directory);

} // namespace lsm::io
