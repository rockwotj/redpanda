// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/work_queue.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <exception>

namespace ssx {

namespace {
ss::logger l{"ssx_queue"};
}

work_queue::work_queue(error_reporter_fn fn)
  : _error_reporter(std::move(fn)) {}

void work_queue::submit(ss::noncopyable_function<ss::future<>()> fn) {
    if (_as.abort_requested()) {
        return;
    }
    _tail = _tail
              .then([this, fn = std::move(fn)]() {
                  if (_as.abort_requested()) {
                      return ss::now();
                  }
                  l.debug("starting task");
                  return fn().finally([] { l.debug("finishing task"); });
              })
              .handle_exception(
                [this](const std::exception_ptr& ex) { _error_reporter(ex); });
}

ss::future<> work_queue::shutdown() {
    _as.request_abort();
    co_await _gate.close();
    co_await std::exchange(_tail, ss::now());
}

void work_queue::submit_after(
  ss::future<> fut, ss::noncopyable_function<ss::future<>()> fn) {
    auto h = _gate.hold();
    ssx::background = std::move(fut).then_wrapped(
      [this, h = std::move(h), fn = std::move(fn)](auto fut) mutable {
          fut.ignore_ready_future();
          if (_as.abort_requested()) {
              // when an abort is requested, stop is
              // responsible for deleting the future,
              // as it needs to wait for them.
              return;
          }
          submit(std::move(fn));
      });
}
} // namespace ssx
