/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "pandaproxy/rest/proxy.h"
#include "pandaproxy/server.h"

#include <seastar/core/future.hh>

namespace pandaproxy::rest {

ss::future<proxy::server::reply_t>
kv_write(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
kv_get(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
kv_scan(proxy::server::request_t rq, proxy::server::reply_t rp);

} // namespace pandaproxy::rest
