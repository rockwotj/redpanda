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

#include "crash_tracker/types.h"

#include "version/version.h"

namespace crash_tracker {

std::string_view format_as(crash_type ct) {
    switch (ct) {
    case crash_type::unknown:
        return "unknown";
    case crash_type::startup_exception:
        return "startup_exception";
    case crash_type::segfault:
        return "segfault";
    case crash_type::abort:
        return "abort";
    case crash_type::illegal_instruction:
        return "illegal_instruction";
    case crash_type::assertion:
        return "assertion";
    case crash_type::oom:
        return "oom";
    }
}

std::ostream& operator<<(std::ostream& os, crash_type ct) {
    return os << format_as(ct);
}

fmt::iterator crash_description::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "Redpanda version: {}. Arch: {}. {}",
      app_version,
      arch,
      crash_message.c_str());

    const auto opt_stacktrace = stacktrace.c_str();
    const auto has_stacktrace = strlen(opt_stacktrace) > 0;
    if (has_stacktrace) {
        it = fmt::format_to(it, " Backtrace: {}.", opt_stacktrace);
    }

    return it;
}

bool is_crash_loop_limit_reached(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const crash_loop_limit_reached&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace crash_tracker
