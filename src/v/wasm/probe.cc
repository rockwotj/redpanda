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

void transform_probe::setup_metrics(ss::sstring transform_name) {
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform_execution"),
      {
        sm::make_histogram(
          "latency_sec",
          sm::description("Data transforms per record latency in seconds"),
          labels,
          [this] { return _transform_latency.public_histogram_logform(); })
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
