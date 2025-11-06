/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/lookup_value_for_key_request.h"
#include "kafka/protocol/schemata/lookup_value_for_key_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka {

struct lookup_value_for_key_request final {
    using api_type = lookup_value_for_key_api;

    lookup_value_for_key_request_data data;

    // extra context from request header set in decode
    api_version version;
    std::optional<kafka::client_id> client_id;
    kafka::client_host client_host;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const lookup_value_for_key_request& r) {
        return os << r.data;
    }
};

struct lookup_value_for_key_response final {
    using api_type = lookup_value_for_key_api;

    lookup_value_for_key_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const lookup_value_for_key_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
