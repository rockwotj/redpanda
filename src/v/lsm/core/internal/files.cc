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

#include "lsm/core/internal/files.h"

#include "absl/strings/numbers.h"
#include "strings/string_switch.h"

#include <seastar/core/format.hh>

namespace lsm::internal {

ss::sstring sst_file_name(file_id id) { return ss::format("{:020}.sst", id()); }

ss::sstring manifest_file_name(file_id id) {
    return ss::format("{:020}.manifest", id());
}

ss::sstring current_file_name() { return "CURRENT"; }

std::optional<parsed_filename> parse_filename(std::string_view filename) {
    if (filename == current_file_name()) {
        return parsed_filename{.type = file_type::current};
    }
    size_t pos = filename.find_last_of('.');
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view name = filename.substr(0, pos);
    std::string_view ext = filename.substr(pos);
    auto maybe_type = string_switch<std::optional<file_type>>(ext)
                        .match(".sst", file_type::sst)
                        .match(".manifest", file_type::manifest)
                        .match(".lsm-staging", file_type::tmp)
                        .default_match(std::nullopt);
    if (!maybe_type) {
        return std::nullopt;
    }
    if (maybe_type == file_type::tmp) {
        return parsed_filename{.type = file_type::tmp};
    }
    uint64_t raw_id = 0;
    if (!absl::SimpleAtoi(name, &raw_id)) {
        return std::nullopt;
    }
    return parsed_filename{.id = file_id{raw_id}, .type = *maybe_type};
}

} // namespace lsm::internal
