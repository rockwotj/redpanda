/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/types.h"

#include "cloud_storage/configuration.h"
#include "config/node_config.h"
#include "model/metadata.h"

namespace {
constexpr auto supported_backends = {model::cloud_storage_backend::aws};
}

namespace cloud_storage::inventory {
std::ostream& operator<<(std::ostream& os, report_generation_frequency rgf) {
    return os << format_as(rgf);
}

std::ostream& operator<<(std::ostream& os, report_format rf) {
    return os << format_as(rf);
}

std::ostream& operator<<(std::ostream& os, inventory_creation_result icr) {
    return os << format_as(icr);
}

bool validate_backend_supported_for_inventory_scrub(
  model::cloud_storage_backend backend) {
    return std::ranges::find(supported_backends, backend)
           != supported_backends.end();
}

} // namespace cloud_storage::inventory
