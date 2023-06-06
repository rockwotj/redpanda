// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include "model/record.h"
#include "model/tests/random_batch.h"
#include "ssx/thread_worker.h"
#include "wasm/probe.h"
#include "wasm/wasm.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/file.hh>

#include <memory>

class wasm_test_fixture {
public:
    wasm_test_fixture();
    wasm_test_fixture(const wasm_test_fixture&) = delete;
    wasm_test_fixture& operator=(const wasm_test_fixture&) = delete;
    wasm_test_fixture(wasm_test_fixture&&) = delete;
    wasm_test_fixture& operator=(wasm_test_fixture&&) = delete;
    ~wasm_test_fixture();

    void load_wasm(const std::string& path);
    model::record_batch make_tiny_batch();
    ss::circular_buffer<model::record_batch>
    transform(const model::record_batch&);

private:
    ssx::thread_worker _worker;
    wasm::service _service;
    wasm::transform::metadata _meta;
};
