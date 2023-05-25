/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "model/tests/random_batch.h"
#include "seastarx.h"
#include "wasm/probe.h"
#include "wasm/wasm.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>

SEASTAR_THREAD_TEST_CASE(test_wasm_transforms_work) {
    wasm::probe p;
    auto wasm_file = ss::util::read_entire_file_contiguous(
                       "golang_identity_transform.wasm")
                       .get0();
    auto engine = wasm::make_wasm_engine(
                    "identity_transform", std::move(wasm_file))
                    .get0();
    auto batch = model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
    });
    auto result_batch = engine->transform(batch.copy(), &p).get0();
    BOOST_CHECK_EQUAL(result_batch.copy_records(), batch.copy_records());
    BOOST_CHECK_EQUAL(result_batch, batch);
}
