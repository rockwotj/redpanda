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

#include "base/seastarx.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace datalake {

struct record_type {
    record_schema_components comps;
    iceberg::struct_type type;
};

class record_translator {
public:
    enum class errc {
        translation_error,
        unexpected_schema,
    };

    virtual record_type build_type(std::optional<resolved_type> val_type) = 0;
    virtual ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers)
      = 0;
    virtual ~record_translator() = default;
};

inline constexpr std::string_view format_as(record_translator::errc e) {
    switch (e) {
    case record_translator::errc::translation_error:
        return "record_translator::errc::translation_error";
    case record_translator::errc::unexpected_schema:
        return "record_translator::errc::unexpected_schema";
    }
}

class key_value_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) override;
    ~key_value_translator() override = default;
};

class structured_data_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) override;
    ~structured_data_translator() override = default;
};

// Switches between key-value and structured translator, depending on if there
// is an input schema.
// XXX: this is a temporary hack for tests to pass as we transition to toggling
// mode with a topic config! Instead, callers should explicitly choose.
class default_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) override;
    ~default_translator() override = default;

private:
    key_value_translator kv_translator;
    structured_data_translator structured_translator;
};

} // namespace datalake

template<>
struct fmt::formatter<datalake::record_translator::errc>
  : fmt::formatter<std::string_view> {
    auto
    format(datalake::record_translator::errc e, fmt::format_context& ctx) const {
        return fmt::formatter<std::string_view>::format(
          datalake::format_as(e), ctx);
    }
};
