/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace transform {

void probe::setup_metrics(ss::sstring transform_name, probe_guages g) {
    wasm::transform_probe::setup_metrics(transform_name);
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform"),
      {
        sm::make_gauge(
          "processors_running",
          std::move(g.num_processors_callback),
          sm::description("Data transform processor instances"),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "processors_input_queue_size",
          std::move(g.input_queue_size_callback),
          sm::description("Data transform processor input queue sizes"),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "processors_output_queue_size",
          std::move(g.output_queue_size_callback),
          sm::description("Data transform processor output queue sizes"),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "processors_engine_memory_usage_bytes",
          std::move(g.engine_memory_usage_callback),
          sm::description("Data transform processor Wasm engine memory usage"),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "processor_read_bytes",
          [this] { return _read_bytes; },
          sm::description(),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "processor_write_bytes",
          [this] { return _write_bytes; },
          sm::description(),
          labels)
          .aggregate({ss::metrics::shard_label}),
      });
}
void probe::increment_write_bytes(uint64_t bytes) { _write_bytes += bytes; }
void probe::increment_read_bytes(uint64_t bytes) { _read_bytes += bytes; }
} // namespace transform
