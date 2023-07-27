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
#include "transform/transform_processor.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "random/simple_time_jitter.h"
#include "transform/logger.h"
#include "transform/transform_manager.h"
#include "utils/prefix_logger.h"
#include "wasm/fwd.h"
#include "wasm/probe.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/defer.hh>

#include <boost/range/irange.hpp>

#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace transform {

namespace {
struct queue_consumer {
    ss::queue<model::record_batch>* queue;
    model::offset latest_offset;
    prefix_logger* logger;

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        latest_offset = model::next_offset(b.last_offset());
        vlog(logger->trace, "read upto input offset {}", b.last_offset());
        co_await queue->push_eventually(std::move(b));
        co_return ss::stop_iteration::no;
    }
    model::offset end_of_stream() { return latest_offset; }
};
// We use a queue with a single element as a notification mechanism
constexpr size_t kQueueBufferSize = 1;
} // namespace

processor::processor(
  cluster::transform_id id,
  model::ntp ntp,
  cluster::transform_metadata meta,
  std::unique_ptr<wasm::engine> engine,
  error_callback cb,
  std::unique_ptr<source> source,
  std::vector<std::unique_ptr<sink>> sinks,
  wasm::probe* probe)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _source(std::move(source))
  , _sinks(std::move(sinks))
  , _error_callback(std::move(cb))
  , _probe(probe)
  , _task(ss::now())
  , _consumer_pings(kQueueBufferSize)
  , _input_queue(kQueueBufferSize)
  , _logger(tlog, ss::format("{}/{}", id, _ntp.tp.partition)) {
    _output_queues.reserve(_meta.output_topics.size());
    for (size_t i = 0; i < _meta.output_topics.size(); ++i) {
        _output_queues.emplace_back(kQueueBufferSize);
    }
}

ss::future<> processor::start() {
    try {
        co_await _engine->start();
        co_await _engine->initialize();
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error starting stm: {}", ex);
        _error_callback(_id, _ntp.tp.partition, _meta);
    }
    register_source_subscriber();
    _task = ss::when_all_succeed(
              run_consumer(),
              run_transform(),
              run_all_producers(),
              run_poll_fallback_loop())
              .discard_result();
}

void processor::register_source_subscriber() {
    _source_notification_id = _source->register_on_write_notification([this]() {
        // Try to push into the queue, dropping if it's full. If there are
        // multiple notifications they get debounced this way.
        _consumer_pings.push({});
    });
}

void processor::unregister_source_subscriber() {
    _source->unregister_on_write_notification(_source_notification_id);
}

ss::future<> processor::run_poll_fallback_loop() {
    // the source notifications are best effort, there are cases when they can
    // be missed, this forces that we periodically poll to pick up any failures.
    try {
        constexpr auto fallback_poll_interval = std::chrono::seconds(30);
        simple_time_jitter<ss::lowres_clock> jitter(fallback_poll_interval);
        while (!_as.abort_requested()) {
            co_await ss::sleep_abortable<ss::lowres_clock>(
              jitter.next_duration(), _as);
            _consumer_pings.push({});
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(
          _logger.warn,
          "error with transform poll loop, in rare cases transforms may be "
          "slow: {}",
          ex);
    }
}

ss::future<> processor::run_transform() {
    try {
        while (!_as.abort_requested()) {
            auto batch = co_await _input_queue.pop_eventually();
            auto transformed = co_await _engine->transform(&batch, _probe);
            co_await _output_queues[0].push_eventually(std::move(transformed));
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform: {}", ex);
        _error_callback(_id, _ntp.tp.partition, _meta);
    }
}

void processor::drain_consumer_pings() {
    while (!_consumer_pings.empty()) {
        _consumer_pings.pop();
    }
}

ss::future<> processor::run_consumer() {
    try {
        auto offset = co_await _source->load_latest_offset();
        vlog(_logger.trace, "starting at offset {}", offset);
        while (!_as.abort_requested()) {
            // Drain the ping queue now so we don't always read twice, we need
            // to do this now so that we don't race with a write and read at the
            // same time.
            drain_consumer_pings();
            // TODO: Failures should cause backoff
            auto reader = co_await _source->read_batch(offset, &_as);
            auto latest_offset = co_await std::move(reader).consume(
              queue_consumer{
                .queue = &_input_queue,
                .latest_offset = offset,
                .logger = &_logger},
              model::no_timeout);
            if (latest_offset == offset) {
                // Wait for a ping before attempting to read after we read and
                // get nothing.
                vlog(
                  _logger.trace,
                  "received no results, waiting for ping at offset {}",
                  offset);
                co_await _consumer_pings.pop_eventually();
                continue;
            }
            offset = latest_offset;
            vlog(_logger.trace, "consumed upto offset {}", offset);
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform consumer: {}", ex);
        _error_callback(_id, _ntp.tp.partition, _meta);
    }
}

ss::future<> processor::run_all_producers() {
    return ss::parallel_for_each(
      boost::irange<size_t>(0, _sinks.size()),
      [this](size_t idx) { return run_producer(idx); });
}

ss::future<> processor::run_producer(size_t idx) {
    const auto& tp_ns = _meta.output_topics[idx];
    const auto& ntp = model::ntp(tp_ns.ns, tp_ns.tp, _ntp.tp.partition);
    auto& queue = _output_queues[idx];
    auto& sink = _sinks[idx];
    try {
        while (!_as.abort_requested()) {
            auto batch = co_await queue.pop_eventually();
            vlog(_logger.trace, "writing output to {}", tp_ns);
            // TODO: Support retries here with backoff
            co_await sink->write(std::move(batch));
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform producer: {}", ex);
        _error_callback(_id, _ntp.tp.partition, _meta);
    }
}

ss::future<> processor::stop() {
    unregister_source_subscriber();
    auto ex = std::make_exception_ptr(ss::abort_requested_exception());
    _as.request_abort_ex(ex);
    _input_queue.abort(ex);
    _consumer_pings.abort(ex);
    for (auto& output_queue : _output_queues) {
        output_queue.abort(ex);
    }
    co_await std::exchange(_task, ss::now());
    co_await _engine->stop();
}
cluster::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }

} // namespace transform
