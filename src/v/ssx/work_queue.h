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

#include "seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>
#include <utility>

namespace ssx {

/**
 * A small utility for running tasks sequentially.
 *
 * TODO: Define and support multiple failure policies, currently errors are
 * always ignored.
 */
class work_queue {
public:
    void submit(ss::noncopyable_function<ss::future<>()> fn) {
        if (_as.abort_requested()) {
            return;
        }
        _tail = _tail.finally([this, fn = std::move(fn)]() {
            if (_as.abort_requested()) {
                return ss::now();
            }
            return fn();
        });
    }
    // Shutdown the queue, waiting for the currently executing task to finish.
    ss::future<> shutdown() {
        _as.request_abort();
        return _tail.finally([] {});
    }

private:
    ss::future<> _tail = ss::now();
    ss::abort_source _as;
};

} // namespace ssx
