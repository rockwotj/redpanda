/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/data_file.h"

namespace datalake::coordinator {

fmt::iterator data_file::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{remote_path: {}, row_count: {}, file_size_bytes: {}, hour_deprecated: "
      "{}, table_schema_id: {}, partition_spec_id: {}}}",
      remote_path,
      row_count,
      file_size_bytes,
      hour_deprecated,
      table_schema_id,
      partition_spec_id);
}

} // namespace datalake::coordinator
