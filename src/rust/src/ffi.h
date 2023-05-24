#pragma once

#include "cxx.h"

#include <functional>
#include <memory>

namespace wasmstar {

class AsyncResult {
};

bool poll_async_result(const std::unique_ptr<AsyncResult>&);

using HostFunc = std::function<void(rust::Slice<uint8_t>, rust::Slice<const uint64_t>, rust::Slice<uint64_t>)>;

void invoke(const std::unique_ptr<HostFunc>&, rust::Slice<uint8_t>, rust::Slice<const uint64_t>, rust::Slice<uint64_t>);

} // namespace wasmstar
