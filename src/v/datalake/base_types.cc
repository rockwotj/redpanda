/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/base_types.h"

namespace datalake {
fmt::iterator local_file_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{relative_path: {}, size_bytes: {}, row_count: {}}}",
      path,
      size_bytes,
      row_count);
}
} // namespace datalake
