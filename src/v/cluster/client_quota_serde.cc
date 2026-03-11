
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "client_quota_serde.h"

#include "serde/rw/envelope.h"
#include "serde/rw/set.h"     // IWYU pragma: keep
#include "serde/rw/sstring.h" // IWYU pragma: keep
#include "utils/to_string.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ostream>

namespace cluster::client_quota {

fmt::iterator
entity_key::part::client_id_default_match::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "client_id_default_match{{}}");
}

fmt::iterator
entity_key::part::client_id_match::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "client_id_match{{value:{}}}", value);
}

fmt::iterator
entity_key::part::user_default_match::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "user_default_match{{}}");
}

fmt::iterator
entity_key::part::user_match::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "user_match{{value:{}}}", value);
}

fmt::iterator
entity_key::part::client_id_prefix_match::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "client_id_prefix_match{{value:{}}}", value);
}

fmt::iterator entity_key::part::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", part);
}

fmt::iterator entity_key::part_v0::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", part);
}

fmt::iterator entity_key::part_t::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", part);
}

fmt::iterator entity_key::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{parts: {}}}", parts);
}

fmt::iterator entity_value::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{producer_byte_rate: {}, consumer_byte_rate: {}, "
      "controller_mutation_rate: {}}}",
      producer_byte_rate,
      consumer_byte_rate,
      controller_mutation_rate);
}

fmt::iterator
entity_value_diff::entry::format_to(fmt::iterator it) const {
    switch (op) {
    case entity_value_diff::operation::upsert:
        return fmt::format_to(
          it, "upsert: {}={}", to_string_view(type), value);
    case entity_value_diff::operation::remove:
        return fmt::format_to(it, "remove: {}", to_string_view(type));
    }
}

fmt::iterator entity_value_diff::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", entries);
}

bool operator==(
  const entity_value_diff::entry& lhs, const entity_value_diff::entry& rhs) {
    if (lhs.op != rhs.op) {
        return false;
    }
    switch (lhs.op) {
    case entity_value_diff::operation::upsert:
        return lhs.type == rhs.type && lhs.value == rhs.value;
    case entity_value_diff::operation::remove:
        return lhs.type == rhs.type;
    }
}

bool entity_key::part::is_v0_compat() const {
    const auto v0_fields = [](const auto&) { return true; };
    const auto not_v0_fields = [](const auto&) { return false; };
    return ss::visit(part, part_v0::visitor{v0_fields, not_v0_fields});
}

// This is used to make entity_part::part_t backwards compatible. Currently,
// serde::variant atomic and not compatible with any other variant type.
// To enable forward-compatible behavior, old version is being deserialized as
// the old type and then converted. The envelope header dictates what variant
// type was serialized. This extra layer can be removed after v26.2
void entity_key::part::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    if (h._version == 0) {
        auto lv0 = read_nested<part_v0::variant>(in, h._bytes_left_limit);
        ss::visit(lv0, [this](auto value) { part = value; });
    } else {
        part = read_nested<part::variant>(in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void entity_key::part::serde_write(iobuf& out) const {
    serde::write(out, part);
}

void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  entity_key::part_t& p,
  const std::size_t bytes_left_limit) {
    using serde::read_nested;
    p = entity_key::part_t{read_nested<entity_key::part>(in, bytes_left_limit)};
}

// This is used to make entity_part::part_t backwards compatible. Currently,
// serde::variant atomic and not compatible with any other variant type.
// To enable backwards-compatible behavior, values existing in the previous
// versions are being serialized as the old type. This cannot happen in
// entity_type::part because it also needs to have an envelope header that is
// version<0>. This extra layer can be removed after v26.2
void tag_invoke(
  serde::tag_t<serde::write_tag>, iobuf& out, const entity_key::part_t& part) {
    const auto v0_write = [&out](const auto& v) {
        serde::write(out, entity_key::part_v0{.part = v});
    };
    const auto write = [&out](const auto& v) {
        serde::write(out, entity_key::part{.part = v});
    };
    ss::visit(part.part, entity_key::part_v0::visitor{v0_write, write});
}

} // namespace cluster::client_quota
