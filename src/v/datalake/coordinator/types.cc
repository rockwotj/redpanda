/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/types.h"

#include "utils/to_string.h"

namespace datalake::coordinator {

std::string_view format_as(errc e) {
    switch (e) {
    case errc::ok:
        return "errc::ok";
    case errc::coordinator_topic_not_exists:
        return "errc::coordinator_topic_not_exists";
    case errc::not_leader:
        return "errc::not_leader";
    case errc::timeout:
        return "errc::timeout";
    case errc::fenced:
        return "errc::fenced";
    case errc::stale:
        return "errc::stale";
    case errc::concurrent_requests:
        return "errc::concurrent_requests";
    case errc::revision_mismatch:
        return "errc::revision_mismatch";
    case errc::incompatible_schema:
        return "errc::incompatible_schema";
    case errc::failed:
        return "errc::failed";
    }
}

fmt::iterator ensure_table_exists_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{errc: {}}}", errc);
}

fmt::iterator ensure_table_exists_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic: {}, topic_revision: {}}}", topic, topic_revision);
}

fmt::iterator
ensure_dlq_table_exists_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{errc: {}}}", errc);
}

fmt::iterator
ensure_dlq_table_exists_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic: {}, topic_revision: {}}}", topic, topic_revision);
}

fmt::iterator
add_translated_data_files_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{errc: {}}}", errc);
}

fmt::iterator
add_translated_data_files_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{partition: {}, topic_revision: {}, files: {}, translation term: {}}}",
      tp,
      topic_revision,
      ranges,
      translator_term);
}

fmt::iterator
fetch_latest_translated_offset_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{errc: {}, offset: {}}}", errc, last_added_offset);
}

fmt::iterator
fetch_latest_translated_offset_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{partition: {}, topic_revision: {}}}", tp, topic_revision);
}

fmt::iterator per_topic_usage_stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic: {}, revision: {}, total_kafka_bytes_processed: {}}}",
      topic,
      revision,
      total_kafka_bytes_processed);
}

fmt::iterator datalake_usage_stats::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{topic_usages: {} }}", topic_usages);
}

fmt::iterator usage_stats_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{errc: {}, stats: {}}}", errc, stats);
}

fmt::iterator usage_stats_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{coordinator_partition: {}}}", coordinator_partition);
}

fmt::iterator get_topic_state_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{errc: {}, topic_states size: {}}}",
      errc,
      topic_states.size());
}

fmt::iterator get_topic_state_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{coordinator_partition: {}, topics_filter: {}}}",
      coordinator_partition,
      topics_filter);
}

fmt::iterator reset_topic_state_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{errc: {}}}", errc);
}

} // namespace datalake::coordinator
