v_cc_library(
  NAME transform
  HDRS
    api.h
  SRCS
    "api.cc"
    "logger.cc"
    "transform_manager.cc"
    "transform_processor.cc"
  DEPS
    v::wasm
    v::storage
    v::cluster
    v::model
    v::pandaproxy_schema_registry
    Seastar::seastar
)

