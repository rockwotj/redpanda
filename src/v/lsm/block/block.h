/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "lsm/block/contents.h"
#include "lsm/core/iterator.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::block {

class block {
public:
    explicit block(ss::lw_shared_ptr<contents>);

    std::unique_ptr<core::iterator> create_iterator();

private:
    ss::lw_shared_ptr<contents> _data;
    uint32_t _restart_offset; // Offset in data_ of restart array
};

} // namespace lsm::block
