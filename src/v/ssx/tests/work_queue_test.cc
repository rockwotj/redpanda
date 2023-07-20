// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "seastarx.h"
#include "ssx/work_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <exception>
#include <stdexcept>
#include <vector>

SEASTAR_THREAD_TEST_CASE(work_queue_order) {
    ssx::work_queue queue([](auto ex) { std::rethrow_exception(ex); });

    std::vector<int> values;
    ss::promise<> p;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit([&] {
        values.push_back(2);
        return ss::now();
    });
    queue.submit([&] {
        values.push_back(3);
        p.set_value();
        return ss::now();
    });

    BOOST_CHECK_EQUAL(values, std::vector<int>{});
    p.get_future().get();
    BOOST_CHECK_EQUAL(values, std::vector<int>({1, 2, 3}));
    queue.shutdown().get();
}

SEASTAR_THREAD_TEST_CASE(failures_do_not_stop_the_queue) {
    int error_count = 0;
    ssx::work_queue queue([&error_count](auto) { ++error_count; });

    std::vector<int> values;
    ss::promise<> p;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit(
      [&]() -> ss::future<> { throw std::runtime_error("oh noes!"); });
    queue.submit([&] {
        values.push_back(3);
        p.set_value();
        return ss::now();
    });

    BOOST_CHECK_EQUAL(values, std::vector<int>{});
    BOOST_CHECK_EQUAL(error_count, 0);
    p.get_future().get();
    BOOST_CHECK_EQUAL(error_count, 1);
    BOOST_CHECK_EQUAL(values, std::vector<int>({1, 3}));
    queue.shutdown().get();
}

SEASTAR_THREAD_TEST_CASE(shutdown_stops_the_queue_immediately) {
    ssx::work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    int a = 1;
    queue.submit([&] {
        a = 2;
        return ss::now();
    });
    queue.shutdown().get();
    BOOST_CHECK_EQUAL(a, 1);
}
