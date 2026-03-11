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

#include "base/seastarx.h"
#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/api_versions_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct api_versions_request final {
    using api_type = api_versions_api;

    api_versions_request_data data;

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct api_versions_response final {
    using api_type = api_versions_api;

    api_versions_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

} // namespace kafka
