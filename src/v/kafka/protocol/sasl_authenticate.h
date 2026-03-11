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
#include "kafka/protocol/schemata/sasl_authenticate_request.h"
#include "kafka/protocol/schemata/sasl_authenticate_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct sasl_authenticate_request final {
    using api_type = sasl_authenticate_api;

    sasl_authenticate_request_data data;

    sasl_authenticate_request() = default;

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

struct sasl_authenticate_response final {
    using api_type = sasl_authenticate_api;

    sasl_authenticate_response_data data;

    sasl_authenticate_response() = default;

    explicit sasl_authenticate_response(sasl_authenticate_response_data data)
      : data(std::move(data)) {}

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
