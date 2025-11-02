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

#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace lsm::internal {

// The level in the LSM tree.
using level = named_type<uint8_t, struct level_tag>;

consteval level operator""_level(unsigned long long val) {
    if (val > level::max()()) {
        // This is consteval, so this is a compile time error
        throw std::exception();
    }
    return level{static_cast<uint8_t>(val)};
}

// The numeric ID of an sst file
using file_id = named_type<uint64_t, struct file_id_tag>;

consteval file_id operator""_file_id(unsigned long long val) {
    return file_id{static_cast<uint64_t>(val)};
}

// Compute the name of an sst file with the given ID.
ss::sstring sst_file_name(file_id);

// Compute the name of a manifest file with the given ID.
ss::sstring manifest_file_name(file_id);

// The name of the CURRENT file (pointer to existing manifest).
ss::sstring current_file_name();

// The type of file that it is.
enum class file_type : uint8_t {
    current,
    manifest,
    sst,
    // Persistence layers are allowed to have arbitrary `*.lsm-staging` files.
    //
    // In reality, this is only for the local disk persistence that is used when
    // creating new CURRENT files.
    //
    // This will always have file ID 0.
    tmp,
};

struct parsed_filename {
    file_id id;
    file_type type;
};

// Parse a filename, returning nullopt if the filename pattern is unknown.
std::optional<parsed_filename> parse_filename(std::string_view filename);

} // namespace lsm::internal
