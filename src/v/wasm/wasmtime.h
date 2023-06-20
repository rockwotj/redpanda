/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "wasm/wasm.h"

#include <memory>
#include <wasm.h>

namespace wasm::wasmtime {

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

struct runtime;

void delete_runtime(runtime*);
std::unique_ptr<runtime, decltype(&delete_runtime)> make_runtime();

/**
 * If this existing thread is running, used for signal handling.
 */
bool is_running();

std::unique_ptr<engine::factory>
compile(runtime*, transform::metadata, std::string_view wasm_source);

} // namespace wasm::wasmtime
