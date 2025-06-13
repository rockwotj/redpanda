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

#include "bytes/iobuf.h"

#include <cstdint>

namespace lsm::block {

// A handle to a block within an SST file.
struct handle {
    uint64_t offset = 0;
    uint64_t size = 0;

    bool operator==(const handle& other) const = default;

    iobuf as_iobuf() const;
};

} // namespace lsm::block
