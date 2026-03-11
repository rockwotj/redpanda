/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include <cstdint>
#include <iostream>
#include <string_view>

namespace cloud_io {

enum class [[nodiscard]] download_result : int32_t {
    success,
    notfound,
    timedout,
    failed,
};

inline constexpr std::string_view format_as(download_result r) {
    switch (r) {
    case download_result::success:
        return "{success}";
    case download_result::notfound:
        return "{key_not_found}";
    case download_result::timedout:
        return "{timed_out}";
    case download_result::failed:
        return "{failed}";
    }
}

enum class [[nodiscard]] upload_result : int32_t {
    success,
    timedout,
    failed,
    cancelled,
};

inline constexpr std::string_view format_as(upload_result r) {
    switch (r) {
    case upload_result::success:
        return "{success}";
    case upload_result::timedout:
        return "{timed_out}";
    case upload_result::failed:
        return "{failed}";
    case upload_result::cancelled:
        return "{cancelled}";
    }
}

std::ostream& operator<<(std::ostream& o, const download_result& r);
std::ostream& operator<<(std::ostream& o, const upload_result& r);

} // namespace cloud_io
