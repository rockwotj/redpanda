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

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/transform.h"
#include "seastarx.h"
#include "transform/io.h"
#include "transform/probe.h"
#include "utils/prefix_logger.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

namespace transform {

/**
 * A processor is the driver of a transform for a single partition.
 *
 * At it's heart it's a fiber that reads->transforms->writes batches
 * from an input ntp to an output ntp.
 */
class processor {
public:
    using error_callback = ss::noncopyable_function<void(
      model::transform_id, model::ntp, model::transform_metadata)>;

    processor(
      model::transform_id,
      model::ntp,
      model::transform_metadata,
      std::unique_ptr<wasm::engine>,
      error_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      probe*);
    processor(const processor&) = delete;
    processor(processor&&) = delete;
    processor& operator=(const processor&) = delete;
    processor& operator=(processor&&) = delete;
    virtual ~processor() = default;

    virtual ss::future<> start();
    virtual ss::future<> stop();

    bool is_running() const;
    model::transform_id id() const;
    const model::ntp& ntp() const;
    const model::transform_metadata& meta() const;
    uint64_t memory_usage() const;

private:
    ss::future<> run_transform();
    ss::future<> run_consumer();
    ss::future<> run_poll_fallback_loop();
    ss::future<> run_all_producers();
    ss::future<> run_producer(size_t);

    void register_source_subscriber();
    void unregister_source_subscriber();
    void drain_consumer_pings();

    model::transform_id _id;
    model::ntp _ntp;
    model::transform_metadata _meta;
    std::unique_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::vector<std::unique_ptr<sink>> _sinks;
    error_callback _error_callback;
    probe* _probe;

    ss::queue<std::monostate> _consumer_pings;
    ss::queue<model::record_batch_reader::data_t> _input_queue;
    std::vector<ss::queue<ss::chunked_fifo<model::record_batch>>>
      _output_queues;

    ss::abort_source _as;
    ss::future<> _task;
    prefix_logger _logger;
    cluster::notification_id_type _source_notification_id
      = cluster::notification_id_type_invalid;
};
} // namespace transform
