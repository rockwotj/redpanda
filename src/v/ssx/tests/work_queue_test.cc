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
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <exception>
#include <stdexcept>
#include <vector>

using namespace std::chrono_literals;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

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

SEASTAR_THREAD_TEST_CASE(delayed_tasks) {
    ssx::work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    int a = 1;
    ss::promise<> p1;
    ss::future<> t1 = p1.get_future();
    queue.submit_delayed<ss::manual_clock>(100ms, [&] {
        a = 2;
        p1.set_value();
        return ss::now();
    });
    ss::promise<> p2;
    ss::future<> t2 = p2.get_future();
    queue.submit_delayed<ss::manual_clock>(10ms, [&] {
        a = 3;
        p2.set_value();
        return ss::now();
    });
    BOOST_CHECK_EQUAL(a, 1);
    ss::manual_clock::advance(5ms);
    BOOST_CHECK(!t1.available());
    BOOST_CHECK(!t2.available());
    BOOST_CHECK_EQUAL(a, 1);
    ss::manual_clock::advance(10ms);
    t2.get();
    BOOST_CHECK(!t1.available());
    BOOST_CHECK_EQUAL(a, 3);
    ss::manual_clock::advance(100ms);
    t1.get();
    BOOST_CHECK_EQUAL(a, 2);
    queue.shutdown().get();
}

SEASTAR_THREAD_TEST_CASE(shutdown_delayed_tasks) {
    ssx::work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    int a = 1;
    queue.submit_delayed<ss::manual_clock>(100ms, [&] {
        a = 2;
        return ss::now();
    });
    queue.shutdown().get();
    BOOST_CHECK_EQUAL(a, 1);
}

SEASTAR_THREAD_TEST_CASE(threaded_queue) {
    ssx::threaded_work_queue q;
    q.start().get();
    int a = q.enqueue<int>([]() { return 1; }).get();
    BOOST_CHECK_EQUAL(a, 1);
    auto [b, c, d, e] = ss::when_all_succeed(
                          q.enqueue<int>([]() { return 1; }),
                          q.enqueue<int>([]() { return 2; }),
                          q.enqueue<int>([]() { return 3; }),
                          q.enqueue<int>([]() { return 4; }))
                          .get();
    BOOST_CHECK_EQUAL(b, 1);
    BOOST_CHECK_EQUAL(c, 2);
    BOOST_CHECK_EQUAL(d, 3);
    BOOST_CHECK_EQUAL(e, 4);
    q.stop().get();
}

SEASTAR_THREAD_TEST_CASE(threaded_queue_start_stop) {
    ssx::threaded_work_queue q;
    for (int i = 0; i < 128; ++i) {
        q.start().get();
        auto [a, stop, b] = ss::when_all(
                              q.enqueue<int>([]() { return 1; }),
                              q.stop(),
                              q.enqueue<int>([]() { return 1; }))
                              .get();
        // These are valid to race with stopping the queue are OK to fail
        a.ignore_ready_future();
        b.ignore_ready_future();
        BOOST_CHECK(!stop.failed());
        stop.get();
    }
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
