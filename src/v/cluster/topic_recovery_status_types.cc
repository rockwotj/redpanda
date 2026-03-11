/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/topic_recovery_status_types.h"

namespace cluster {

void recovery_request_params::populate(
  const std::optional<cloud_storage::recovery_request>& r) {
    if (r.has_value()) {
        topic_names_pattern = r->topic_names_pattern();
        retention_bytes = r->retention_bytes();
        retention_ms = r->retention_ms();
    }
}

fmt::iterator recovery_request_params::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic_names_pattern: {}, retention_bytes: {}, retention_ms: {}}}",
      topic_names_pattern.has_value() ? topic_names_pattern.value() : "none",
      retention_bytes.has_value()
        ? fmt::format("{}", retention_bytes.value())
        : "none",
      retention_ms.has_value() ? fmt::format("{}", retention_ms.value())
                               : "none");
}

bool status_response::is_active() const {
    if (status_log.empty()) {
        return false;
    }

    return status_log.back().state
           != cloud_storage::topic_recovery_service::state::inactive;
}

} // namespace cluster
