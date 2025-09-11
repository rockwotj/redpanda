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
#include "lsm/core/internal/files.h"

#include <cstddef>

namespace lsm::internal {

struct options {
    struct level_config {
        // The level number in the database.
        internal::level number;

        fmt::iterator format_to(fmt::iterator) const;
    };
    // The levels and their configuration in the database,
    // this will be sorted by level number and also will be monotonically
    // increasing from level 0 to level N (configurable).
    std::vector<level_config> levels;

    constexpr static size_t default_level_one_compaction_trigger = 4;
    size_t level_one_compaction_trigger = default_level_one_compaction_trigger;

    fmt::iterator format_to(fmt::iterator) const;
};
} // namespace lsm::internal
