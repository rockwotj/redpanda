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

#include "bytes/iobuf_parser.h"
#include "json/document.h"
#include "test_utils/fixture.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <format>
#include <unordered_set>
#include <vector>

FIXTURE_TEST(test_wasi_works, wasm_test_fixture) {
    load_wasm("wasi.wasm");
    auto batch = make_tiny_batch();
    auto result = transform(batch);
    const auto& result_records = result.copy_records();
    BOOST_CHECK_EQUAL(result_records.size(), 1);
    iobuf_const_parser parser(result_records.front().value());
    const auto& value = parser.read_string(parser.bytes_left());
    json::Document doc;
    doc.Parse(value);
    std::vector<std::string> program_args;
    auto args = doc["Args"].GetArray();
    for (const auto& arg : args) {
        std::string_view v{arg.GetString(), arg.GetStringLength()};
        program_args.emplace_back(v);
    }
    std::vector<std::string> expected_args{meta().name()};
    BOOST_CHECK_EQUAL(program_args, expected_args);

    std::vector<std::string> environment_variables;
    auto env = doc["Env"].GetArray();
    for (const auto& var : env) {
        std::string_view v{var.GetString(), var.GetStringLength()};
        environment_variables.emplace_back(v);
    }
    // The order here doesn't matter, so sort the values.
    std::sort(environment_variables.begin(), environment_variables.end());
    std::vector<std::string> expected_env{
      ss::format("REDPANDA_INPUT_TOPIC={}", meta().input_topic.tp()),
      ss::format(
        "REDPANDA_OUTPUT_TOPIC={}", meta().output_topics.begin()->tp()),
    };
    BOOST_CHECK_EQUAL(environment_variables, expected_env);

    using namespace std::chrono;
    milliseconds now_ms = milliseconds(wasm_test_fixture::NOW());
    nanoseconds now_ns = duration_cast<nanoseconds>(now_ms);
    BOOST_CHECK_EQUAL(doc["NowNanos"].GetInt64(), now_ns.count());

    BOOST_CHECK_EQUAL(doc["RandomNumber"].GetInt(), 240963032);
}
