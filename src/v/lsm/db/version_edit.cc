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

#include "lsm/db/version_edit.h"

#include "utils/to_string.h" // IWYU pragma: keep

namespace lsm::db {

fmt::iterator file_meta_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id:{},file_size:{},smallest:{},largest:{},allowed_seeks:{}}}",
      id,
      file_size,
      smallest,
      largest,
      allowed_seeks);
}

fmt::iterator version_edit::mutation::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{removed_files:{},added_files:{},compact_pointer:{}}}",
      fmt::join(removed_files, ","),
      fmt::join(added_files, ","),
      compact_pointer);
}

fmt::iterator version_edit::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{mutations_by_level:{},next_file_number:{},last_seq_num:{}}}",
      fmt::join(_mutations_by_level, ","),
      _has_next_file_number ? std::make_optional(_next_file_number)
                            : std::nullopt,
      _has_last_seq_num ? std::make_optional(_last_seq_num) : std::nullopt);
}

} // namespace lsm::db
