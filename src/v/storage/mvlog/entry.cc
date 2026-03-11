// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/entry.h"

namespace storage::experimental::mvlog {

std::ostream& operator<<(std::ostream& o, entry_type t) {
    return o << format_as(t);
}

fmt::iterator entry_header::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{header_crc: {}, body_size: {}, entry_type: {}}}",
      header_crc,
      body_size,
      type);
}

} // namespace storage::experimental::mvlog
