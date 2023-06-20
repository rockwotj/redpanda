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
    v::pandaproxy_schema_registry
    wasmtime
    Seastar::seastar
)

add_subdirectory(tests)
