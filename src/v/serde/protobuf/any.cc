/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/protobuf/any.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace serde::pb {

namespace {

struct parser_fns {
    any_parser_fn json;
    any_parser_fn proto;
};

constinit absl::Mutex registered_parsers_mu{absl::kConstInit};
absl::flat_hash_map<std::string_view, parser_fns> registered_parsers;

} // namespace

any_parser_registation::any_parser_registation(
  static_str full_name,
  any_parser_fn proto_parser,
  any_parser_fn json_parser) noexcept {
    registered_parsers_mu.lock();
    registered_parsers.insert(
      std::make_pair(
        full_name, parser_fns{.json = json_parser, .proto = proto_parser}));
    registered_parsers_mu.unlock();
}

} // namespace serde::pb
