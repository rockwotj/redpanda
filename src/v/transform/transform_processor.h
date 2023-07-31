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
#include "utils/prefix_logger.h"
#include "wasm/api.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <variant>

namespace transform {

using error_callback = ss::noncopyable_function<void(
  cluster::transform_id, model::partition_id, cluster::transform_metadata)>;

class processor {
public:
    processor(
      cluster::transform_id,
      model::ntp,
      cluster::transform_metadata,
      std::unique_ptr<wasm::engine>,
      error_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      wasm::transform_probe*);

    ss::future<> start();
    ss::future<> stop();

    cluster::transform_id id() const;
    const model::ntp& ntp() const;

private:
    ss::future<> run_transform();
    ss::future<> run_consumer();
    ss::future<> run_poll_fallback_loop();
    ss::future<> run_all_producers();
    ss::future<> run_producer(size_t);

    void register_source_subscriber();
    void unregister_source_subscriber();
    void drain_consumer_pings();

    cluster::transform_id _id;
    model::ntp _ntp;
    cluster::transform_metadata _meta;
    std::unique_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::vector<std::unique_ptr<sink>> _sinks;
    error_callback _error_callback;
    wasm::transform_probe* _probe;

    ss::abort_source _as;
    ss::future<> _task;
    ss::queue<std::monostate> _consumer_pings;
    ss::queue<model::record_batch> _input_queue;
    std::vector<ss::queue<model::record_batch>> _output_queues;
    prefix_logger _logger;
    cluster::notification_id_type _source_notification_id
      = cluster::notification_id_type_invalid;
};
} // namespace transform
