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
#include "transform/transform_stm.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "transform/logger.h"
#include "transform/transform_manager.h"
#include "wasm/fwd.h"
#include "wasm/probe.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
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
#include <vector>

namespace transform {

namespace {
struct queue_consumer {
    ss::queue<model::record_batch>* queue;
    model::offset latest_offset;

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        latest_offset = model::next_offset(b.last_offset());
        vlog(tlog.trace, "read upto input offset {}", b.last_offset());
        co_await queue->push_eventually(std::move(b));
        co_return ss::stop_iteration::no;
    }
    model::offset end_of_stream() { return latest_offset; }
};
} // namespace

stm::stm(
  cluster::transform_id id,
  model::ntp ntp,
  cluster::transform_metadata meta,
  std::unique_ptr<wasm::engine> engine,
  error_callback cb,
  source::factory* source_factory,
  sink::factory* sink_factory)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _error_callback(std::move(cb))
  , _running(false)
  , _task(ss::now())
  , _input_queue(1) {
    _source = source_factory->create(_ntp);
    _sinks.reserve(_meta.output_topics.size());
    _output_queues.reserve(_meta.output_topics.size());
    for (const auto& output_topic : _meta.output_topics) {
        _sinks.push_back(sink_factory->create(
          model::ntp(output_topic.ns, output_topic.tp, _ntp.tp.partition)));
        _output_queues.emplace_back(1);
    }
}

ss::future<> stm::start() {
    try {
        co_await _engine->start();
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "error starting stm: {}", ex);
        _error_callback(_id, _meta);
    }
    _running = true;
    _task = ss::when_all_succeed(
              run_consumer(), run_transform(), run_all_producers())
              .discard_result();
}

ss::future<> stm::run_transform() {
    wasm::probe probe;
    // TODO: setup metrics without conflicts, what's the story here?
    // auto _ = ss::defer([&probe] { probe.clear_metrics(); });
    // probe.setup_metrics(_meta.name());
    try {
        while (_running) {
            auto batch = co_await _input_queue.pop_eventually();
            auto transformed = co_await _engine->transform(&batch, &probe);
            co_await _output_queues[0].push_eventually(std::move(transformed));
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "error running transform: {}", ex);
        _error_callback(_id, _meta);
    }
}
ss::future<> stm::run_consumer() {
    try {
        auto offset = co_await _source->load_latest_offset();
        vlog(
          tlog.trace, "starting {} at {} offset {}", _meta.name, _ntp, offset);
        while (_running) {
            auto reader = co_await _source->read_batch(offset);
            auto latest_offset = co_await std::move(reader)->consume(
              queue_consumer{.queue = &_input_queue, .latest_offset = offset},
              model::no_timeout);
            if (latest_offset == offset) {
                constexpr auto delay = std::chrono::seconds(1);
                // TODO: Add jitter or use notifications on when to read.
                co_await ss::sleep(delay);
                continue;
            }
            offset = latest_offset;
            vlog(
              tlog.trace,
              "{} consumed {} upto offset {}",
              _meta.name,
              _ntp,
              offset);
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "error running transform consumer: {}", ex);
        _error_callback(_id, _meta);
    }
}
ss::future<> stm::run_all_producers() {
    return ss::parallel_for_each(
      boost::irange<size_t>(0, _sinks.size()),
      [this](size_t idx) { return run_producer(idx); });
}
ss::future<> stm::run_producer(size_t idx) {
    const auto& tp_ns = _meta.output_topics[idx];
    const auto& ntp = model::ntp(tp_ns.ns, tp_ns.tp, _ntp.tp.partition);
    auto& queue = _output_queues[idx];
    auto& sink = _sinks[idx];
    try {
        while (_running) {
            auto batch = co_await queue.pop_eventually();
            vlog(tlog.trace, "writing {} output to {}", _meta.name, tp_ns);
            co_await sink->write(std::move(batch));
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const std::exception& ex) {
        vlog(tlog.warn, "error running transform producer: {}", ex);
        _error_callback(_id, _meta);
    }
}
ss::future<> stm::stop() {
    _running = false;
    auto ex = std::make_exception_ptr(ss::abort_requested_exception());
    _input_queue.abort(ex);
    for (auto& output_queue : _output_queues) {
        output_queue.abort(ex);
    }
    co_await std::exchange(_task, ss::now());
    co_await _engine->stop();
}
cluster::transform_id stm::id() const { return _id; }
const model::ntp& stm::ntp() const { return _ntp; }

} // namespace transform
