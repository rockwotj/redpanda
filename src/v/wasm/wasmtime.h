#pragma once

#include "wasm/wasm.h"

namespace wasm::wasmtime {

/**
 * If this existing thread is running, used for signal handling.
 */
bool is_running();

std::unique_ptr<engine> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source);

} // namespace wasm::wasmtime
