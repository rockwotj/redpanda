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

FIXTURE_TEST(test_transform_panic, wasm_test_fixture) {
#ifdef NDEBUG
    auto engine = load_engine("transform_panic.wasm");
    auto batch = make_tiny_batch();
    BOOST_CHECK_EXCEPTION(
      engine->transform(std::move(batch), probe()).get(),
      wasm::wasm_exception,
      [](const wasm::wasm_exception& ex) {
          return ex.error_code() == wasm::errc::user_code_failure;
      });
#endif
}
