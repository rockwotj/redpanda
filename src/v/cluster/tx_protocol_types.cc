/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/tx_protocol_types.h"

#include "utils/to_string.h"

#include <fmt/format.h>
namespace cluster {

fmt::iterator commit_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      ntp,
      pid,
      tx_seq,
      timeout);
}

fmt::iterator commit_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ec {}}}", ec);
}

fmt::iterator abort_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      ntp,
      pid,
      tx_seq,
      timeout);
}

fmt::iterator abort_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ec {}}}", ec);
}

fmt::iterator begin_group_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} group_id {} pid {} tx_seq {} timeout {} tm_partition: {}}}",
      ntp,
      group_id,
      pid,
      tx_seq,
      timeout,
      tm_partition);
}

fmt::iterator begin_group_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{etag {} ec {}}}", etag, ec);
}

fmt::iterator prepare_group_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} group_id {} etag {} pid {} tx_seq {} timeout {}}}",
      ntp,
      group_id,
      etag,
      pid,
      tx_seq,
      timeout);
}

fmt::iterator prepare_group_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ec {}}}", ec);
}

fmt::iterator commit_group_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      ntp,
      pid,
      tx_seq,
      group_id,
      timeout);
}

fmt::iterator commit_group_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ec {}}}", ec);
}

fmt::iterator abort_group_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      ntp,
      pid,
      tx_seq,
      group_id,
      timeout);
}

fmt::iterator abort_group_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ec {}}}", ec);
}

fmt::iterator find_coordinator_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{tid {}}}", tid);
}

fmt::iterator find_coordinator_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{coordinator {} ntp {} ec {}}}", coordinator, ntp, ec);
}

fmt::iterator fetch_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{tx_id: {}, term: {}, tm: {}}}", tx_id, term, tm);
}

fmt::iterator fetch_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ec: {}, pid: {}, last_pid: {}, tx_seq: {}, timeout_ms: {}, status: "
      "{}, partitions: {}, groups: {}}}",
      ec,
      pid,
      last_pid,
      tx_seq,
      timeout_ms.count(),
      static_cast<int32_t>(status),
      partitions,
      groups);
}

fmt::iterator fetch_tx_reply::tx_group::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{etag: {}, group_id: {}}}", etag, group_id);
}

fmt::iterator
fetch_tx_reply::tx_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{etag: {}, ntp: {}, revision: {}}}", etag, ntp, topic_revision);
}

fmt::iterator
add_partitions_tx_request::topic::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic: {}, partitions: {}}}", name, partitions);
}

fmt::iterator begin_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ ntp: {}, pid: {}, tx_seq: {}, tm_partition: {}}}",
      ntp,
      pid,
      tx_seq,
      tm_partition);
}

fmt::iterator begin_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ ntp: {}, etag: {}, ec: {} }}", ntp, etag, ec);
}

fmt::iterator prepare_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ ntp: {}, etag: {}, tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      ntp,
      etag,
      tm,
      pid,
      tx_seq,
      timeout);
}

fmt::iterator prepare_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ ec: {} }}", ec);
}

fmt::iterator init_tm_tx_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ tx_id: {}, transaction_timeout_ms: {}, timeout: {} }}",
      tx_id,
      transaction_timeout_ms,
      timeout);
}

fmt::iterator init_tm_tx_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ pid: {}, ec: {} }}", pid, ec);
}

fmt::iterator try_abort_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      tm,
      pid,
      tx_seq,
      timeout);
}

fmt::iterator try_abort_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ commited: {}, aborted: {}, ec: {} }}",
      bool(commited),
      bool(aborted),
      ec);
}

fmt::iterator
idempotent_request_info::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ first: {}, last: {}, term: {} }}",
      first_sequence,
      last_sequence,
      term);
}

fmt::iterator producer_state_info::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ pid: {}, inflight_requests: {}, finished: {}, begin offset: {}, end "
      "offset: {}, sequence: {}, timeout: "
      "{}, coordinator: {}, last_update: {}, group: {} }}",
      pid,
      inflight_requests,
      finished_requests,
      tx_begin_offset,
      tx_end_offset,
      tx_seq,
      tx_timeout,
      coordinator_partition,
      last_update,
      group_id);
}

fmt::iterator get_producers_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ ec: {}, producers: {} , count: {} }}",
      error_code,
      producers,
      producer_count);
}

fmt::iterator get_producers_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ ntp: {} timeout: {}, max_producers_to_include: {} }}",
      ntp,
      timeout,
      max_producers_to_include);
}

} // namespace cluster
