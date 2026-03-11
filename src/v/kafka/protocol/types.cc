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
#include "kafka/protocol/types.h"

namespace kafka {

std::ostream& operator<<(std::ostream& os, describe_configs_type t) {
    return os << format_as(t);
}

std::ostream&
operator<<(std::ostream& os, describe_client_quotas_match_type t) {
    return os << format_as(t);
}

std::ostream& operator<<(std::ostream& os, coordinator_type t) {
    return os << format_as(t);
}

std::ostream& operator<<(std::ostream& os, config_resource_type t) {
    return os << format_as(t);
}

std::ostream& operator<<(std::ostream& os, describe_configs_source s) {
    return os << format_as(s);
}

std::ostream& operator<<(std::ostream& os, config_resource_operation t) {
    return os << format_as(t);
}

std::ostream& operator<<(std::ostream& os, scram_mechanism m) {
    return os << format_as(m);
}

} // namespace kafka
