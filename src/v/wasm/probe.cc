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

#include <seastar/core/thread_cputime_clock.hh>

namespace wasm {

cpu_measurement::cpu_measurement(probe* p)
  : _start(ss::thread_cputime_clock::now())
  , _probe(p) {}

cpu_measurement::~cpu_measurement() {
    auto end = ss::thread_cputime_clock::now();
    _probe->record_cpu_time(end - _start);
}

void probe::setup_metrics(ss::sstring transform_name) {
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("redpanda:wasm"),
      {
        sm::make_histogram(
          "latency_us",
          sm::description("Wasm Latency"),
          labels,
          [this] { return _transform_latency.seastar_histogram_logform(); })
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "cpu_busy_seconds_total",
          sm::description("Wasm CPU time"),
          labels,
          [this] { return _cpu_time; })
          .aggregate(aggregate_labels),
        sm::make_counter(
          "count",
          [this] { return _transform_count; },
          sm::description("Wasm transforms total count"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "errors",
          [this] { return _transform_errors; },
          sm::description("Wasm errors"),
          labels)
          .aggregate(aggregate_labels),
      });
}

} // namespace wasm
