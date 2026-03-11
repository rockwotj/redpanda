/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/describe_groups_request.h"
#include "kafka/protocol/schemata/describe_groups_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct describe_groups_request final {
    using api_type = describe_groups_api;

    describe_groups_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct describe_groups_response final {
    using api_type = describe_groups_api;

    describe_groups_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    static described_group
    make_empty_described_group(group_id g, error_code e) {
        return described_group{
          .error_code = e,
          .group_id = std::move(g),
          .group_state = "",
          .protocol_type = kafka::protocol_type(),
          .protocol_data = "",
        };
    }

    static described_group make_dead_described_group(group_id g) {
        return described_group{
          .error_code = error_code::none,
          .group_id = std::move(g),
          .group_state = ss::sstring(group_state_name_dead),
          .protocol_type = kafka::protocol_type(),
          .protocol_data = "",
        };
    }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

} // namespace kafka
