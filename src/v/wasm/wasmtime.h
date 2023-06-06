#pragma once

#include "wasm/wasm.h"

#include <memory>

namespace wasm::wasmtime {

std::unique_ptr<runtime> make_runtime();

/**
 * If this existing thread is running, used for signal handling.
 */
bool is_running();

std::unique_ptr<engine::factory> compile(
  runtime*, std::string_view wasm_module_name, std::string_view wasm_source);

} // namespace wasm::wasmtime
