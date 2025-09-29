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
#include "base/units.h"
#include "lsm/core/internal/files.h"

#include <cstddef>

namespace lsm::internal {

struct options {
    struct level_config {
        // The level number in the database.
        internal::level number;

        fmt::iterator format_to(fmt::iterator) const;
    };
    constexpr static auto default_max_level = 6_level;
    static constexpr std::vector<level_config> make_default_levels() {
        std::vector<level_config> levels;
        levels.reserve(default_max_level() + 1);
        for (auto lvl = 0_level; lvl <= default_max_level; ++lvl) {
            levels.emplace_back(lvl);
        }
        return levels;
    }
    // The levels and their configuration in the database,
    // this will be sorted by level number and also will be monotonically
    // increasing from level 0 to level N (configurable).
    std::vector<level_config> levels = make_default_levels();
    internal::level max_level() const { return levels.back().number; }

    // At what point do we start throttling writes?
    constexpr static size_t default_level_zero_slowdown_writes_trigger = 8;
    size_t level_zero_slowdown_writes_trigger
      = default_level_zero_slowdown_writes_trigger;
    // At what point do we halt writes?
    constexpr static size_t default_level_zero_stop_writes_trigger = 12;
    size_t level_zero_stop_writes_trigger
      = default_level_zero_stop_writes_trigger;

    // How big to let memtable accumulate before flushing.
    constexpr static size_t default_write_buffer_size = 16_MiB;
    size_t write_buffer_size = default_write_buffer_size;

    // When do we trigger compaction into L1
    constexpr static size_t default_level_one_compaction_trigger = 4;
    size_t level_one_compaction_trigger = default_level_one_compaction_trigger;

    // Write up to this amount of bytes to a file before switching to a new one.
    // Increasing this provides better file system efficiency with larger files,
    // but the downside of increasing this is longer compactions and longer
    // latency/performance hiccups.
    size_t max_file_size = 2_GiB;

    size_t target_file_size() const { return max_file_size; }
    size_t max_grandparent_overlap_bytes() const {
        static constexpr size_t multiplier = 10;
        return multiplier * target_file_size();
    }
    size_t expanded_compaction_byte_size_limit() const {
        static constexpr size_t multiplier = 25;
        return multiplier * target_file_size();
    }

    constexpr static uint32_t default_max_open_files = 1000;
    uint32_t max_open_files = default_max_open_files;

    constexpr static size_t default_block_cache_size = 10_MiB;
    size_t block_cache_size = default_block_cache_size;

    // We arrange to automatically compact after a file after a certain
    // number of seeks. Let's assume:
    // (1) One seek costs 200us
    // (2) Writing or reading 1MiB costs 1ms (1GiB/s)
    // (3) A compaction of 1MiB does 25MiB of IO:
    //       1MiB read from this level
    //       10-12MiB read from next level (boundaries my be
    //       misaligned)
    //       10-12MiB written to next level
    // This imples that 125 seeks cost the same as the compaction of
    // 1MB of data. I.e., one seek costs approximately the same as
    // the compaction of 8KiB of data.
    constexpr static size_t default_compact_after_seek_bytes = 8_KiB;
    size_t compact_after_seek_bytes = default_compact_after_seek_bytes;

    fmt::iterator format_to(fmt::iterator) const;
};
} // namespace lsm::internal
