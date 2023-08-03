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
#include "model/record_batch_reader.h"
#include "seastarx.h"
#include "ssx/work_queue.h"
#include "transform/fwd.h"
#include "transform/io.h"
#include "utils/prefix_logger.h"
#include "wasm/api.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <variant>
#include <vector>

namespace transform {

using is_retryable = ss::bool_class<struct is_retryable_tag>;

using error_callback = ss::noncopyable_function<void(
  cluster::transform_id,
  model::partition_id,
  cluster::transform_metadata,
  is_retryable)>;

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
    uint64_t input_queue_size() const;
    uint64_t output_queue_size() const;
    uint64_t engine_memory_usage_size_bytes() const;

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
    ss::queue<model::record_batch_reader::data_t> _input_queue;
    std::vector<ss::queue<ss::chunked_fifo<model::record_batch>>>
      _output_queues;
    prefix_logger _logger;
    cluster::notification_id_type _source_notification_id
      = cluster::notification_id_type_invalid;
};
} // namespace transform
