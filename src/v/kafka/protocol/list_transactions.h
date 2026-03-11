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
#include "kafka/protocol/schemata/list_transactions_request.h"
#include "kafka/protocol/schemata/list_transactions_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct list_transactions_request final {
    using api_type = list_transactions_api;

    list_transactions_request_data data;

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

struct list_transactions_response final {
    using api_type = list_transactions_api;

    list_transactions_response_data data;

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
