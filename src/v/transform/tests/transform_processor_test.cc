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

#include "cloud_roles/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/fixture.h"
#include "transform/io.h"
#include "transform/transform_processor.h"
#include "units.h"
#include "utils/notification_list.h"
#include "utils/uuid.h"
#include "wasm/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/test/tools/old/interface.hpp>

#include <exception>
#include <iterator>
#include <memory>
#include <unistd.h>
#include <vector>

namespace {

constexpr auto my_transform_id = cluster::transform_id(42);
// NOLINTBEGIN(cert-err58-cpp)
static const auto my_ntp = model::random_ntp();
static const auto my_metadata = cluster::transform_metadata{
  .name = cluster::transform_name("xform"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {model::random_topic_namespace()},
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(9),
  .failed_partitions = {}};
// NOLINTEND(cert-err58-cpp)

class fake_wasm_engine : public wasm::engine {
public:
    ss::future<model::record_batch>
    transform(model::record_batch batch, wasm::transform_probe*) override {
        co_return batch;
    };

    ss::future<> start() override { return ss::now(); }
    ss::future<> initialize() override { return ss::now(); }
    ss::future<> stop() override { return ss::now(); }

    std::string_view function_name() const override {
        return my_metadata.name();
    }
    uint64_t memory_usage_size_bytes() const override {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
        return 64_KiB;
    };
};

class fake_source : public transform::source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source(model::offset initial_offset)
      : _batches(max_queue_size)
      , _latest_offset(initial_offset) {}

    cluster::notification_id_type register_on_write_notification(
      ss::noncopyable_function<void()> cb) override {
        return _subscriptions.register_cb(std::move(cb));
    }
    void unregister_on_write_notification(
      cluster::notification_id_type id) override {
        return _subscriptions.unregister_cb(id);
    }
    ss::future<model::offset> load_latest_offset() override {
        co_return _latest_offset;
    }
    ss::future<model::record_batch_reader>
    read_batch(model::offset offset, ss::abort_source* as) override {
        BOOST_CHECK_EQUAL(offset, _latest_offset);
        if (!_batches.empty()) {
            model::record_batch_reader::data_t batches;
            while (!_batches.empty()) {
                batches.push_back(_batches.pop());
            }
            _latest_offset = model::next_offset(batches.back().last_offset());
            co_return model::make_memory_record_batch_reader(
              std::move(batches));
        }
        auto sub = as->subscribe(
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [this](const std::optional<std::exception_ptr>& ex) noexcept {
              _batches.abort(ex.value_or(
                std::make_exception_ptr(ss::abort_requested_exception())));
          });
        auto batch = co_await _batches.pop_eventually();
        _latest_offset = model::next_offset(batch.last_offset());
        sub->unlink();
        co_return model::make_memory_record_batch_reader(std::move(batch));
    }

    ss::future<> push_batch(model::record_batch batch) {
        co_await _batches.push_eventually(std::move(batch));
        if (!_corked) {
            _subscriptions.notify();
        }
    }

    // Don't immediately notify when batches are pushed
    void cork() { _corked = true; }
    // Notify that batches are ready and start notifying for every batch again.
    void uncork() {
        _corked = false;
        _subscriptions.notify();
    }

private:
    bool _corked = false;
    ss::queue<model::record_batch> _batches;
    model::offset _latest_offset;
    notification_list<
      ss::noncopyable_function<void()>,
      cluster::notification_id_type>
      _subscriptions;
};

class fake_sink : public transform::sink {
    static constexpr size_t max_queue_size = 64;

public:
    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override {
        for (auto& batch : batches) {
            co_await _batches.push_eventually(std::move(batch));
        }
    }

    ss::future<model::record_batch> read() { return _batches.pop_eventually(); }

private:
    ss::queue<model::record_batch> _batches{max_queue_size};
};

class processor_fixture {
public:
    processor_fixture() {
        std::unique_ptr<wasm::engine> engine
          = std::make_unique<fake_wasm_engine>();
        auto src = std::make_unique<fake_source>(_offset);
        _src = src.get();
        auto sink = std::make_unique<fake_sink>();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        _sinks.push_back(sink.get());
        sinks.push_back(std::move(sink));
        _p = std::make_unique<transform::processor>(
          my_transform_id,
          my_ntp,
          my_metadata,
          std::move(engine),
          [](
            cluster::transform_id,
            model::partition_id,
            const cluster::transform_metadata&,
            transform::is_retryable) {},
          std::move(src),
          std::move(sinks),
          nullptr);
        _p->start().get();
    }

    ~processor_fixture() { _p->stop().get(); }

    model::record_batch make_tiny_batch() {
        return model::test::make_random_batch(model::test::record_batch_spec{
          .offset = _offset++, .allow_compression = false, .count = 1});
    }
    void push_batch(model::record_batch batch) {
        _src->push_batch(std::move(batch)).get();
    }
    void cork() { _src->cork(); }
    void uncork() { _src->uncork(); }
    model::record_batch read_batch() { return _sinks[0]->read().get(); }

private:
    static constexpr model::offset start_offset = model::offset(9);
    model::offset _offset = start_offset;
    std::unique_ptr<transform::processor> _p;
    fake_source* _src;
    std::vector<fake_sink*> _sinks;
};

} // namespace

FIXTURE_TEST(process_one, processor_fixture) {
    auto batch = make_tiny_batch();
    push_batch(batch.share());
    auto returned = read_batch();
    BOOST_CHECK_EQUAL(batch, returned);
}

FIXTURE_TEST(process_many, processor_fixture) {
    std::vector<model::record_batch> batches;
    constexpr int num_batches = 32;
    std::generate_n(std::back_inserter(batches), num_batches, [this] {
        return make_tiny_batch();
    });
    cork();
    for (auto& b : batches) {
        push_batch(b.share());
    }
    uncork();
    for (auto& b : batches) {
        auto returned = read_batch();
        BOOST_CHECK_EQUAL(b, returned);
    }
}
