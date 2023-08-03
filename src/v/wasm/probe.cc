/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/util/noncopyable_function.hh>

namespace wasm {

void transform_probe::setup_metrics(
  ss::sstring transform_name, transform_guages g) {
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
      });
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform_execution"),
      {
        sm::make_histogram(
          "latency_us",
          sm::description("Data transforms per record latency"),
          labels,
          [this] { return _transform_latency.seastar_histogram_logform(1); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "errors",
          [this] { return _transform_errors; },
          sm::description("Data transform invocation errors"),
          labels)
          .aggregate({ss::metrics::shard_label}),
      });
}

} // namespace wasm
