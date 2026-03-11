/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "types.h"

#include "util.h"
#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace pandaproxy::schema_registry {

std::ostream& operator<<(std::ostream& os, const schema_type& v) {
    return os << format_as(v);
}

std::ostream& operator<<(std::ostream& os, const output_format& v) {
    return os << format_as(v);
}

std::ostream& operator<<(std::ostream& os, const reference_format& v) {
    return os << format_as(v);
}

fmt::iterator seq_marker::format_to(fmt::iterator it) const {
    if (seq.has_value() && node.has_value()) {
        return fmt::format_to(
          it,
          "seq={} node={} version={} key_type={}",
          *seq,
          *node,
          version,
          to_string_view(key_type));
    } else {
        return fmt::format_to(
          it,
          "unsequenced version={} key_type={}",
          version,
          to_string_view(key_type));
    }
}

fmt::iterator schema_definition::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "type: {}, definition: {}, references: {}, metadata: {}",
      to_string_view(type()),
      // TODO BP: Prevent this linearization
      to_string(shared_raw()),
      refs(),
      meta());
}

std::ostream& operator<<(std::ostream& os, const schema_reference& ref) {
    fmt::print(os, "{}", ref);
    return os;
}

bool operator<(const schema_reference& lhs, const schema_reference& rhs) {
    return std::tie(lhs.name, lhs.sub, lhs.version)
           < std::tie(rhs.name, rhs.sub, rhs.version);
}

fmt::iterator subject_schema::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "subject: {}, {}", sub(), def());
}

fmt::iterator compatibility_result::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "is_compat: {}, messages: {}", is_compat, messages);
}

fmt::iterator schema_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ properties: {{ {} }} }}", properties);
}

namespace {
std::pair<context_subject, is_qualified> parse_subject(std::string_view input) {
    // If qualified subject parsing is disabled, treat everything as literal
    if (!enable_qualified_subjects::get()) {
        return {
          context_subject{default_context, subject{input}}, is_qualified::no};
    }

    // Check for qualified syntax: starts with ":."
    if (input.starts_with(":.")) {
        // Find the second colon that separates context from subject
        auto second_colon = input.find(':', 2);

        if (second_colon == std::string_view::npos) {
            // No second colon, so only context is provided
            return {
              context_subject{context{input.substr(1)}, subject{}},
              is_qualified::yes};
        }

        // Both context and subject are provided
        auto ctx_str = input.substr(1, second_colon - 1);
        auto sub_str = input.substr(second_colon + 1);

        return {
          context_subject{context{ctx_str}, subject{sub_str}},
          is_qualified::yes};
    }

    // Default case: unqualified subject or invalid qualified syntax
    return {context_subject{default_context, subject{input}}, is_qualified::no};
}
} // namespace

context_subject context_subject::from_string(std::string_view input) {
    return parse_subject(input).first;
}

context_subject_reference
context_subject_reference::from_string(std::string_view input) {
    auto [sub, qualified] = parse_subject(input);
    return context_subject_reference{
      .sub = std::move(sub),
      .qualified = qualified,
    };
}

context_subject
context_subject_reference::resolve(const context& parent_ctx) const {
    if (qualified) {
        // Qualified reference: use its own context
        return sub;
    }
    // Unqualified reference: inherit parent's context
    return context_subject{parent_ctx, sub.sub};
}

ss::sstring context_subject_reference::to_string() const {
    return ssx::sformat("{}", *this);
}

fmt::iterator context_subject_reference::format_to(fmt::iterator it) const {
    if (qualified) {
        return fmt::format_to(it, ":{}:{}", sub.ctx, sub.sub);
    }
    return fmt::format_to(it, "{}", sub);
}

} // namespace pandaproxy::schema_registry
