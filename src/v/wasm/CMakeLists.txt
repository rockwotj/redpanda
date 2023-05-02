v_cc_library(
  NAME wasm
  HDRS
    wasm.h
  SRCS
    "wasm.cc"
  DEPS
    wasmedge
    v::storage
    v::model
    Seastar::seastar
)
