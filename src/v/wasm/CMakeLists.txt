v_cc_library(
  NAME wasm
  HDRS
    wasm.h
  SRCS
    "wasm.cc"
    "probe.cc"
    "wasi.cc"
    "wasmtime.cc"
    "ffi.cc"
  DEPS
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
