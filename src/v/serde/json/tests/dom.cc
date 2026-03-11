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

#include "serde/json/tests/dom.h"

#include "absl/strings/escaping.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"

#include <seastar/util/variant_utils.hh>

namespace serde::json::test::dom {

namespace {

std::string iobuf_as_string(const iobuf& b) {
    return absl::CHexEscape(b.linearize_to_string());
}

fmt::iterator debug_format_value(
  fmt::iterator it, const value& v, int base_indent = 0) {
    it = fmt::format_to(it, "{:>{}}", "", base_indent);

    ss::visit(
      v.data(),
      [&it](const iobuf& v) {
          it = fmt::format_to(it, "string({})", iobuf_as_string(v));
      },
      [&it, base_indent](const json_object& v) {
          it = fmt::format_to(it, "object(");
          for (const auto& [k, v] : v) {
              it = fmt::format_to(
                it, "\n{:>{}}key({}) :\n", "", base_indent + 2, iobuf_as_string(k));
              it = debug_format_value(it, v, base_indent + 4);
          }
          if (v.size() == 0) {
              it = fmt::format_to(it, ")");
          } else {
              it = fmt::format_to(it, "\n{:>{}}", "", base_indent);
              it = fmt::format_to(it, ")");
          }
      },
      [&it, base_indent](const json_array& v) {
          it = fmt::format_to(it, "array(");
          for (const auto& v : v) {
              it = fmt::format_to(it, "\n");
              it = debug_format_value(it, v, base_indent + 2);
          }
          if (v.size() == 0) {
              it = fmt::format_to(it, ")");
          } else {
              it = fmt::format_to(it, "\n{:>{}}", "", base_indent);
              it = fmt::format_to(it, ")");
          }
      },
      [&it](const null_t&) { it = fmt::format_to(it, "null"); },
      [&it](bool b) { it = fmt::format_to(it, "{}", b ? "true" : "false"); },
      [&it](int64_t i) { it = fmt::format_to(it, "int({})", i); },
      [&it](double d) { it = fmt::format_to(it, "double({})", d); });

    return it;
}

} // namespace

fmt::iterator value::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "json_value(");
    it = debug_format_value(it, *this, 0);
    return fmt::format_to(it, ")");
}

} // namespace serde::json::test::dom
