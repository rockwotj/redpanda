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

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/fixture.h"
#include "transform/io.h"
#include "transform/tests/test_fixture.h"
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

using namespace transform;

class processor_fixture {
public:
    processor_fixture() {
        std::unique_ptr<wasm::engine> engine
          = std::make_unique<testing::fake_wasm_engine>();
        auto src = std::make_unique<testing::fake_source>(_offset);
        _src = src.get();
        auto sink = std::make_unique<testing::fake_sink>();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        _sinks.push_back(sink.get());
        sinks.push_back(std::move(sink));
        _p = std::make_unique<transform::processor>(
          testing::my_transform_id,
          testing::my_ntp,
          testing::my_metadata,
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
    processor_fixture(const processor_fixture&) = delete;
    processor_fixture(processor_fixture&&) = delete;
    processor_fixture& operator=(const processor_fixture&) = delete;
    processor_fixture& operator=(processor_fixture&&) = delete;

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
    testing::fake_source* _src;
    std::vector<testing::fake_sink*> _sinks;
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
