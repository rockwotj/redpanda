/* Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/rpc/serde.h"

#include "model/record.h"
#include "utils/to_string.h"

#include <seastar/core/chunked_fifo.hh>

#include <fmt/format.h>

namespace transform::rpc {
transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, model::record_batch b)
  : tp(std::move(tp)) {
    batches.reserve(1);
    batches.push_back(std::move(b));
}

transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> b)
  : tp(std::move(tp))
  , batches(std::move(b)) {}

transformed_topic_data transformed_topic_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {tp, std::move(shared)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<transformed_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}

fmt::iterator offset_commit_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ kvs: {}, coordinator: {} }}", kvs.size(), coordinator);
}

fmt::iterator offset_commit_response::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ errc: {} }}", errc);
}

fmt::iterator find_coordinator_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ num_keys: {} }}", keys.size());
}

fmt::iterator find_coordinator_response::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ coordinators: {}, errors: {} }}",
      coordinators.size(),
      errors.size());
}

fmt::iterator generate_report_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ }}");
}

fmt::iterator generate_report_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ transforms: {} }}", report.transforms.size());
}

fmt::iterator offset_fetch_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ keys: {}, coordinator: {} }}",
      keys.size(),
      coordinator);
}

fmt::iterator offset_fetch_response::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ errc: {}, results: {} }}",
      errors.size(),
      results.size());
}

fmt::iterator load_wasm_binary_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ offset: {}, timeout: {} }}", offset, timeout);
}

fmt::iterator load_wasm_binary_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ data_size: {}, errc: {} }}",
      data()->size_bytes(),
      ec);
}

fmt::iterator delete_wasm_binary_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ errc: {} }}", ec);
}

fmt::iterator delete_wasm_binary_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ key: {}, timeout: {} }}", key, timeout);
}

fmt::iterator store_wasm_binary_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ errc: {}, stored: {} }}", ec, stored);
}

fmt::iterator
stored_wasm_binary_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ key: {}, offset: {} }}", key, offset);
}

fmt::iterator store_wasm_binary_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ data_size: {}, timeout: {} }}",
      data()->size_bytes(),
      timeout);
}

fmt::iterator produce_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ topic_data: {}, timeout: {} }}",
      fmt::join(topic_data, ", "),
      timeout);
}

fmt::iterator produce_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ results: {} }}", fmt::join(results, ", "));
}

fmt::iterator
transformed_topic_data_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ errc: {}, tp: {} }}", err, tp);
}

fmt::iterator transformed_topic_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ tp: {}, batches_size: {} }}", tp, batches.size());
}

fmt::iterator list_commits_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ partition: {} }}", partition);
}

fmt::iterator list_commits_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ ec: {}, map_size: {} }}", errc, map.size());
}

fmt::iterator delete_commits_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ partition: {}, transform_ids_size: {} }}",
      partition,
      ids.size());
}

fmt::iterator delete_commits_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ ec: {} }}", errc);
}

} // namespace transform::rpc
