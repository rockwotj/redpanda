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

#include "datalake/schema_identifier.h"

namespace datalake {

class table_creator {
public:
    enum class errc {
        incompatible_schema,
        // The operation failed because of a subsystem failure.
        failed,
        // The system is shutting down.
        shutting_down,
    };

    virtual ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic&,
      model::revision_id topic_revision,
      record_schema_components) const
      = 0;

    virtual ss::future<checked<std::nullopt_t, errc>> ensure_dlq_table(
      const model::topic&, model::revision_id topic_revision) const
      = 0;

    virtual ~table_creator() = default;
};

inline constexpr std::string_view format_as(table_creator::errc e) {
    switch (e) {
    case table_creator::errc::incompatible_schema:
        return "table_creator::errc::incompatible_schema";
    case table_creator::errc::failed:
        return "table_creator::errc::failed";
    case table_creator::errc::shutting_down:
        return "table_creator::errc::shutting_down";
    }
}

} // namespace datalake

template<>
struct fmt::formatter<datalake::table_creator::errc>
  : fmt::formatter<std::string_view> {
    auto
    format(datalake::table_creator::errc e, fmt::format_context& ctx) const {
        return fmt::formatter<std::string_view>::format(
          datalake::format_as(e), ctx);
    }
};
