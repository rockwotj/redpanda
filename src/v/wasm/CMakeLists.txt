v_cc_library(
  NAME wasm
  HDRS
    wasm.h
  SRCS
    "wasm.cc"
    "probe.cc"
    "wasi.cc"
    "wasmtime.cc"
    "wasmedge.cc"
    "ffi.cc"
    "rp_module.cc"
  DEPS
    wasmedge
    v::storage
    v::http
    v::model
    v::wasmstar
    Seastar::seastar
  COPTS
  # Surely there are better ways of doing this...
  -I${CMAKE_CURRENT_BINARY_DIR}/../../rust
)

add_subdirectory(tests)
