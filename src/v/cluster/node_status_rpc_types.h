/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "model/metadata.h"
#include "serde/rw/envelope.h"

#include <fmt/ostream.h>

namespace cluster {

struct node_status_metadata
  : serde::envelope<
      node_status_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id node_id;

    auto serde_fields() { return std::tie(node_id); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{node_id:{}}}", node_id);
    }
};

struct node_status_request
  : serde::envelope<
      node_status_request,
      serde::version<0>,
      serde::compat_version<0>> {
    node_status_metadata sender_metadata;

    auto serde_fields() { return std::tie(sender_metadata); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{sender_metadata: {}}}", sender_metadata);
    }
};

struct node_status_reply
  : serde::
      envelope<node_status_reply, serde::version<0>, serde::compat_version<0>> {
    node_status_metadata replier_metadata;

    auto serde_fields() { return std::tie(replier_metadata); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{replier_metadata: {}}}", replier_metadata);
    }
};

} // namespace cluster
