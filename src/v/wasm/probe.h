/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "ssx/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <memory>

namespace wasm {

// Per transform probe
class transform_probe {
public:
    using hist_t = log_hist_public;

    transform_probe() = default;
    transform_probe(const transform_probe&) = delete;
    transform_probe& operator=(const transform_probe&) = delete;
    transform_probe(transform_probe&&) = delete;
    transform_probe& operator=(transform_probe&&) = delete;
    ~transform_probe() = default;

    std::unique_ptr<hist_t::measurement> latency_measurement() {
        return _transform_latency.auto_measure();
    }
    void transform_error() { ++_transform_errors; }

    void setup_metrics(ss::sstring transform_name);
    void clear_metrics() { _public_metrics.clear(); }

protected:
    // NOLINTNEXTLINE(*-non-private-member-variables-in-classes)
    ssx::metrics::metric_groups _public_metrics
      = ssx::metrics::metric_groups::make_public();

private:
    uint64_t _transform_errors{0};
    hist_t _transform_latency;
};
} // namespace wasm
