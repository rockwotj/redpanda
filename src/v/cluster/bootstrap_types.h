// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/format_to.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <vector>

namespace cluster {

struct cluster_bootstrap_info_request
  : serde::envelope<
      cluster_bootstrap_info_request,
      serde::version<0>,
      serde::compat_version<0>> {
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{}}");
    }

    auto serde_fields() { return std::tie(); }
};

struct cluster_bootstrap_info_reply
  : serde::envelope<
      cluster_bootstrap_info_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    model::broker broker;
    cluster_version version;
    std::vector<net::unresolved_address> seed_servers;
    bool empty_seed_starts_cluster;
    std::optional<model::cluster_uuid> cluster_uuid;
    model::node_uuid node_uuid;

    auto serde_fields() {
        return std::tie(
          broker,
          version,
          seed_servers,
          empty_seed_starts_cluster,
          cluster_uuid,
          node_uuid);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{broker: {}, version: {}, seed_servers: {}, "
          "empty_seed_starts_cluster: {}, cluster_uuid: {}, node_uuid: {}}}",
          broker,
          version,
          seed_servers,
          empty_seed_starts_cluster,
          cluster_uuid,
          node_uuid);
    }
};

} // namespace cluster
