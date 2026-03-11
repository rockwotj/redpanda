/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/translated_offset_range.h"

namespace datalake::coordinator {

fmt::iterator translated_offset_range::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{start_offset: {}, last_offset: {}, files: {}, dlq_files: {}, "
      "kafka_bytes_processed: {}}}",
      start_offset,
      last_offset,
      files,
      dlq_files,
      kafka_bytes_processed);
}

} // namespace datalake::coordinator
