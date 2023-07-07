/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_cputime_clock.hh>

#include <chrono>
#include <memory>

namespace wasm {

class probe;

class cpu_measurement {
public:
    explicit cpu_measurement(probe*);
    cpu_measurement(const cpu_measurement&) = delete;
    cpu_measurement& operator=(const cpu_measurement&) = delete;
    cpu_measurement(cpu_measurement&&) = delete;
    cpu_measurement& operator=(cpu_measurement&&) = delete;
    ~cpu_measurement();

private:
    ss::thread_cputime_clock::time_point _start;
    probe* _probe;
};

// Per transform probe
class probe {
public:
    probe() = default;
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() = default;

    std::unique_ptr<hdr_hist::measurement> latency_measurement() {
        return _transform_latency.auto_measure();
    }
    std::unique_ptr<cpu_measurement> cpu_time_measurement() {
        return std::make_unique<cpu_measurement>(this);
    }
    void record_cpu_time(const std::chrono::duration<double>& d) {
        _cpu_time += d.count();
    }
    void transform_complete() { ++_transform_count; }
    void transform_error() { ++_transform_errors; }

    void setup_metrics(ss::sstring transform_name);
    void clear_metrics() { _public_metrics.clear(); }

private:
    uint64_t _transform_count{0};
    uint64_t _transform_errors{0};
    double _cpu_time{0};
    hdr_hist _transform_latency;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};
} // namespace wasm
