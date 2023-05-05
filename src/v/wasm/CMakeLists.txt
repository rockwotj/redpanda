v_cc_library(
  NAME wasm
  HDRS
    wasm.h
  SRCS
    "wasm.cc"
  DEPS
    wasmedge
    v::storage
    v::http
    v::model
    Seastar::seastar
)

add_subdirectory(tests)
