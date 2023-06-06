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

#include "wasm/wasm.h"

#include <memory>
#include <wasm.h>

class wasm_engine_t;
extern void wasm_engine_delete(wasm_engine_t*);

namespace wasm::wasmtime {

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

class runtime {
public:
    explicit runtime(wasm_engine_t* e)
      : _engine(e) {}

    wasm_engine_t* get() const { return _engine.get(); }

private:
    handle<wasm_engine_t, wasm_engine_delete> _engine;
};

std::unique_ptr<runtime> make_runtime();

/**
 * If this existing thread is running, used for signal handling.
 */
bool is_running();

std::unique_ptr<engine::factory> compile(
  runtime*, std::string_view wasm_module_name, std::string_view wasm_source);

} // namespace wasm::wasmtime
