/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kafka/server/datalake_usage_api.h"

#include "utils/to_string.h"

namespace kafka {

datalake_usage_api::usage_stats::usage_stats(const usage_stats& other) {
    if (other.topic_stats) {
        topic_stats = other.topic_stats->copy();
    } else {
        topic_stats.reset();
    }
    missing_reason = other.missing_reason;
}

datalake_usage_api::usage_stats& datalake_usage_api::usage_stats::operator=(
  const datalake_usage_api::usage_stats& other) {
    if (this != &other) {
        if (other.topic_stats) {
            topic_stats = other.topic_stats->copy();
        } else {
            topic_stats.reset();
        }
        missing_reason = other.missing_reason;
    }
    return *this;
}

fmt::iterator datalake_usage_api::topic_usage::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ topic: {} revision: {} kafka_bytes_processed: {} }}",
      topic,
      revision,
      kafka_bytes_processed);
}

fmt::iterator datalake_usage_api::usage_stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ topic_stats: {}, missing_stats_reason: {} }}",
      topic_stats,
      missing_reason);
}

} // namespace kafka
