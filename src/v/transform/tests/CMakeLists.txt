rp_test(
  UNIT_TEST
  BINARY_NAME transform_processor
  SOURCES
    transform_processor_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::transform
    v::model_test_utils
  ARGS "-- -c 1"
  LABELS transform
)

# rp_test(
#   UNIT_TEST
#   BINARY_NAME wasi_logs
#   SOURCES
#     wasi_logs_test.cc
#   LIBRARIES 
#     Boost::unit_test_framework
#     v::wasm
#   DEFINITIONS BOOST_TEST_DYN_LINK
#   LABELS wasm
# )
