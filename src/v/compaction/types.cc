// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/types.h"

#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <ostream>

namespace compaction {

fmt::iterator compaction_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{max_removable_local_log_offset:{}, "
      "max_tombstone_remove_offset:{}, "
      "max_tx_end_remove_offset:{}, "
      "should_sanitize:{}, "
      "tombstone_retention_ms:{}, "
      "tx_retention_ms:{}}}",
      max_removable_local_log_offset,
      max_tombstone_remove_offset,
      max_tx_end_remove_offset,
      sanitizer_config,
      tombstone_retention_ms,
      tx_retention_ms);
}

fmt::iterator stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ batches_processed: {}, batches_discarded: {}, "
      "records_discarded: {}, expired_tombstones_discarded: {}, "
      "non_compactible_batches: {}{}}}",
      batches_processed,
      batches_discarded,
      records_discarded,
      expired_tombstones_discarded,
      non_compactible_batches,
      control_batches_discarded > 0
        ? fmt::format(
            ", control_batches_discarded: {}", control_batches_discarded)
        : "");
}

} // namespace compaction
