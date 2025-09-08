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

#include "lsm/db/version_set.h"

namespace lsm::db {

fmt::iterator version::format_to(fmt::iterator it) const {
    // For example:
    //   --- level 1 ---
    //   17:234['a' .. 'e']
    //   20:31['a' .. 'e']
    for (const auto& [level, files] : _files) {
        it = fmt::format_to(it, "--- level {} ---\n", level);
        for (const auto& file : files) {
            it = fmt::format_to(
              it,
              "{}:{}['{}' .. '{}']\n",
              file->id,
              file->file_size,
              file->smallest,
              file->largest);
        }
    }
    return it;
}

void version_set::set_current(ss::lw_shared_ptr<version> new_version) {
    weak_intrusive_list<version>::push_front(&_current, std::move(new_version));
}

} // namespace lsm::db
