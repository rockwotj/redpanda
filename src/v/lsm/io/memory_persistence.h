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

#include "lsm/io/persistence.h"

namespace lsm::io {

class memory_persistence : persistence {
    class impl;

public:
    memory_persistence();
    memory_persistence(const memory_persistence&) = delete;
    memory_persistence(memory_persistence&&) = delete;
    memory_persistence& operator=(const memory_persistence&) = delete;
    memory_persistence& operator=(memory_persistence&&) = delete;
    ~memory_persistence() override;

    ss::future<optional_pointer<sequential_file_reader>>
    open_sequential_reader(std::string_view name) override;

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(std::string_view name) override;

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(std::string_view name) override;

    ss::future<> remove_file(std::string_view name) override;

    ss::coroutine::experimental::generator<ss::sstring> list_files() override;

    ss::future<> close() override;

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::io
