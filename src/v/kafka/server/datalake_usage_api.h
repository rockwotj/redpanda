/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <cstdint>

namespace kafka {

/**
 * Interface for computing and reporting datalake usage statistics
 */
class datalake_usage_api {
public:
    /**
     * Per-topic usage information
     */
    struct topic_usage
      : serde::
          envelope<topic_usage, serde::version<0>, serde::compat_version<0>> {
        model::topic topic{};
        /// Current revision number
        /// Each time a topic is recreated, the revision is updated.
        model::revision_id revision{};
        /// Total kafka bytes processed so far
        /// that resulted in data conversion into iceberg format
        /// Persisted across restarts.
        uint64_t kafka_bytes_processed{0};

        friend bool operator==(const topic_usage&, const topic_usage&)
          = default;
        fmt::iterator format_to(fmt::iterator) const;

        auto serde_fields() {
            return std::tie(topic, revision, kafka_bytes_processed);
        }
    };

    enum class stats_missing_reason : uint8_t {
        /// No error, stats are available
        none = 0,
        /// Disabled by configuration
        feature_disabled = 1,
        /// Error collecting usage stats
        collection_error = 2,
        /// not controller leader
        not_controller_leader = 3,
    };

    friend constexpr std::string_view
    format_as(stats_missing_reason r) {
        switch (r) {
        case stats_missing_reason::none:
            return "none";
        case stats_missing_reason::feature_disabled:
            return "feature_disabled";
        case stats_missing_reason::collection_error:
            return "collection_error";
        case stats_missing_reason::not_controller_leader:
            return "not_controller_leader";
        }
    }
    friend std::ostream&
    operator<<(std::ostream& os, const stats_missing_reason& r) {
        return os << format_as(r);
    }

    /**
     * Usage statistics for a datalake
     */
    struct usage_stats
      : serde::
          envelope<usage_stats, serde::version<0>, serde::compat_version<0>> {
        /// Per-topic usage information
        /// If unset, check `stats_missing_reason`
        std::optional<chunked_vector<topic_usage>> topic_stats;

        stats_missing_reason missing_reason
          = stats_missing_reason::feature_disabled;

        usage_stats() = default;
        usage_stats(const usage_stats&);
        usage_stats(usage_stats&&) = default;
        usage_stats& operator=(const usage_stats&);
        usage_stats& operator=(usage_stats&&) = default;

        friend bool operator==(const usage_stats&, const usage_stats&)
          = default;
        fmt::iterator format_to(fmt::iterator) const;

        auto serde_fields() { return std::tie(topic_stats, missing_reason); }
    };

    virtual ~datalake_usage_api() = default;

    /**
     * Compute current usage statistics for the datalake
     *
     * @return Current datalake usage statistics for all topics
     */
    virtual ss::future<usage_stats> compute_usage(ss::abort_source&) = 0;
};
} // namespace kafka
