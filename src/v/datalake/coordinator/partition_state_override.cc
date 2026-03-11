/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/partition_state_override.h"

namespace datalake::coordinator {

fmt::iterator
partition_state_override::format_to(fmt::iterator it) const {
    if (last_committed.has_value()) {
        return fmt::format_to(
          it, "{{last_committed: {}}}", last_committed.value());
    } else {
        return fmt::format_to(it, "{{last_committed: nullopt}}");
    }
}

} // namespace datalake::coordinator
