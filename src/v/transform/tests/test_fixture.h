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
#include "model/tests/randoms.h"
#include "transform/io.h"
#include "units.h"
#include "utils/notification_list.h"
#include "wasm/api.h"
#include "wasm/probe.h"

namespace transform::testing {

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
    }

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

class fake_source : public source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source(model::offset initial_offset)
      : _batches(max_queue_size)
      , _latest_offset(initial_offset) {}

    cluster::notification_id_type register_on_write_notification(
      ss::noncopyable_function<void()> cb) override;
    void
    unregister_on_write_notification(cluster::notification_id_type id) override;
    ss::future<model::offset> load_latest_offset() override;
    ss::future<model::record_batch_reader>
    read_batch(model::offset offset, ss::abort_source* as) override;

    ss::future<> push_batch(model::record_batch batch);

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

class fake_sink : public sink {
    static constexpr size_t max_queue_size = 64;

public:
    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override;

    ss::future<model::record_batch> read();

private:
    ss::queue<model::record_batch> _batches{max_queue_size};
};

} // namespace transform::testing
