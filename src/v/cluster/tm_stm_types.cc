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
#include "cluster/tm_stm_types.h"

#include "model/timeout_clock.h"

#include <seastar/core/lowres_clock.hh>

#include <optional>

namespace cluster {

namespace {
template<typename... Args>
bool is_one_of(tx_status status, Args&&... args) {
    return ((args == status) || ...);
}

} // namespace

state_transition_error::state_transition_error(tx_status from, tx_status to)
  : from(from)
  , to(to) {}

bool is_state_transition_valid(
  const tx_metadata& current, tx_status target_status) {
    /**
     * The state transition validation logic is really a simple definition of
     * transaction fsm. The validation checks it the current state is a valid
     * precursor of the requested one.
     */
    switch (target_status) {
    case tx_status::empty:
        // ready is an initial state a transaction can never go back to that
        // state
        return is_one_of(current.status, tx_status::empty);
    case tx_status::ongoing:
        return is_one_of(current.status, tx_status::empty, tx_status::ongoing);
    case tx_status::preparing_commit:
        return is_one_of(
          current.status,
          tx_status::empty,
          tx_status::ongoing,
          tx_status::preparing_commit);
    case tx_status::completed_commit:
        return is_one_of(
          current.status,
          tx_status::preparing_commit,
          tx_status::completed_commit);
    case tx_status::preparing_abort:
        return is_one_of(
          current.status,
          tx_status::empty,
          tx_status::ongoing,
          tx_status::preparing_abort);
    case tx_status::preparing_internal_abort:
        return is_one_of(
          current.status,
          tx_status::ongoing,
          tx_status::preparing_internal_abort);
    case tx_status::tombstone:
        return is_one_of(
          current.status,
          tx_status::tombstone,
          tx_status::completed_commit,
          tx_status::completed_abort);
    case tx_status::completed_abort:
        return is_one_of(
          current.status,
          tx_status::preparing_internal_abort,
          tx_status::preparing_abort,
          tx_status::completed_abort);
    }

    __builtin_unreachable();
}

bool tx_metadata::is_finished() const {
    return status == tx_status::completed_commit
           || status == tx_status::completed_abort;
}

std::string_view tx_metadata::get_status() const {
    switch (status) {
    case tx_status::ongoing:
        return "ongoing";
    case tx_status::preparing_commit:
        return "preparing_commit";
    case tx_status::completed_commit:
        return "completed_commit";
    case tx_status::preparing_abort:
        return "preparing_abort";
    case tx_status::preparing_internal_abort:
        return "expired";
    case tx_status::empty:
        return "empty";
    case tx_status::tombstone:
        return "tombstone";
    case tx_status::completed_abort:
        return "completed_abort";
    }
}

std::string_view tx_metadata::get_kafka_status() const {
    switch (status) {
    case tx_status::ongoing: {
        if (groups.empty() && partitions.empty()) {
            return "Empty";
        }
        return "Ongoing";
    }
    case tx_status::preparing_commit:
        return "PrepareCommit";
    case tx_status::completed_commit:
        return "CompleteCommit";
    case tx_status::preparing_abort:
        return "PrepareAbort";
    case tx_status::preparing_internal_abort:
        // https://issues.apache.org/jira/browse/KAFKA-6119
        // https://github.com/apache/kafka/commit/501a5e262702bcc043724cb9e1f536e16a66399e
        return "PrepareEpochFence";
    case tx_status::empty:
        return "Empty";
    case tx_status::tombstone:
        return "Dead";
    case tx_status::completed_abort:
        return "CompleteAbort";
    }
}

std::chrono::milliseconds tx_metadata::get_staleness() const {
    auto now = ss::lowres_system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      now - last_update_ts);
}

bool tx_metadata::delete_partition(const tx_partition& part) {
    return std::erase_if(
             partitions,
             [part](const auto& partition) {
                 return partition.ntp == part.ntp
                        && partition.etag == part.etag;
             })
           > 0;
}

std::optional<state_transition_error>
tx_metadata::try_update_status(tx_status requested) {
    auto is_valid = is_state_transition_valid(*this, requested);
    if (!is_valid) {
        return state_transition_error(status, requested);
    }
    status = requested;
    last_update_ts = ss::lowres_system_clock::now();
    return std::nullopt;
}

std::string_view format_as(tx_status status) {
    switch (status) {
    case tx_status::ongoing:
        return "ongoing";
    case tx_status::preparing_abort:
        return "preparing_abort";
    case tx_status::preparing_commit:
        return "preparing_commit";
    case tx_status::completed_commit:
        return "completed_commit";
    case tx_status::preparing_internal_abort:
        return "expired";
    case tx_status::empty:
        return "empty";
    case tx_status::tombstone:
        return "tombstone";
    case tx_status::completed_abort:
        return "completed_abort";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& o, tx_status status) {
    return o << format_as(status);
}

fmt::iterator
tx_metadata::tx_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ntp: {}, etag: {}, revision: {}}}", ntp, etag, topic_revision);
}

fmt::iterator tx_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, status: {}, pid: {}, last_pid: {}, etag: {}, seq: {}, "
      "partitions: {}}}",
      id,
      status,
      pid,
      last_pid,
      etag,
      tx_seq,
      fmt::join(partitions, ", "));
}

fmt::iterator
state_transition_error::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "Can not update transaction state from {} to {} as this transition is "
      "invalid",
      from,
      to);
}

} // namespace cluster
