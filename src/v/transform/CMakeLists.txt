v_cc_library(
  NAME transform
  HDRS
    api.h
  SRCS
    "api.cc"
    "probe.cc"
    "logger.cc"
    "transform_manager.cc"
    "transform_processor.cc"
  DEPS
    v::transform_rpc
    v::wasm
    v::storage
    v::cluster
    v::model
    v::kafka
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(rpc)
add_subdirectory(tests)
