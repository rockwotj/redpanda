/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "test_utils/fixture.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/testing/thread_test_case.hh>

#include <exception>

FIXTURE_TEST(test_wasm_transforms_work, wasm_test_fixture) {
    auto engine = load_engine("golang_identity_transform.wasm");
    auto batch = make_tiny_batch();
    auto result_batch = engine->transform(batch.copy(), probe()).get();
    BOOST_CHECK_EQUAL(result_batch.copy_records(), batch.copy_records());
    BOOST_CHECK_EQUAL(result_batch, batch);
}
