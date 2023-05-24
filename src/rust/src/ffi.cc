#include "ffi.h"

namespace wasmstar {

bool poll_async_result(const std::unique_ptr<AsyncResult>&) {
    return false;
}

void invoke(
  const std::unique_ptr<HostFunc>& fn,
  rust::Slice<uint8_t> memory,
  rust::Slice<const uint64_t> params,
  rust::Slice<uint64_t> results) {
    return (*fn)(memory, params, results);
}

} // namespace wasmstar
