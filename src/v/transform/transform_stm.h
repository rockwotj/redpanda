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

#pragma once

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "ssx/work_queue.h"
#include "transform/fwd.h"
#include "transform/io.h"
#include "wasm/api.h"
#include "wasm/fwd.h"

#include <seastar/core/future-util.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>

namespace transform {

using error_callback = ss::noncopyable_function<void(
  cluster::transform_id, cluster::transform_metadata)>;

class stm {
public:
    stm(
      cluster::transform_id,
      model::ntp,
      cluster::transform_metadata,
      std::unique_ptr<wasm::engine>,
      error_callback,
      source::factory*,
      sink::factory*);

    ss::future<> start();
    ss::future<> stop();

    cluster::transform_id id() const;
    const model::ntp& ntp() const;

private:
    ss::future<> run_transform();
    ss::future<> run_consumer();
    ss::future<> run_all_producers();
    ss::future<> run_producer(size_t);

    cluster::transform_id _id;
    model::ntp _ntp;
    cluster::transform_metadata _meta;
    std::unique_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::vector<std::unique_ptr<sink>> _sinks;
    error_callback _error_callback;

    ss::future<> _task;
    ss::queue<model::record_batch> _input_queue;
    std::vector<ss::queue<model::record_batch>> _output_queues;
};
} // namespace transform
