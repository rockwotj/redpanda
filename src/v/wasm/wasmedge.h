
#pragma once

#include "wasm/wasm.h"

#include <memory>

namespace wasm::wasmedge {

ss::future<std::unique_ptr<engine>> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source);

}
