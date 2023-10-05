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

#include "wasm/probe.h"

#include <functional>

namespace transform {

struct probe_guages {
    std::function<uint64_t()> num_processors_callback;
    std::function<uint64_t()> engine_memory_usage_callback;
};

/** A per transform probe. */
class probe : public wasm::transform_probe {
public:
    void setup_metrics(ss::sstring transform_name, probe_guages);

    void increment_read_bytes(uint64_t bytes);
    void increment_write_bytes(uint64_t bytes);
    void increment_failure();

private:
    uint64_t _read_bytes = 0;
    uint64_t _write_bytes = 0;
    uint64_t _failures = 0;
};

} // namespace transform
