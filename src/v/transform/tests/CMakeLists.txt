v_cc_library(
  NAME transform_test_fixture
  HDRS 
    test_fixture.h
  SRCS 
    test_fixture.cc
  DEPS
    v::transform
    Seastar::seastar
    v::model_test_utils
    Boost::unit_test_framework
  )

rp_test(
  UNIT_TEST
  BINARY_NAME transform_processor
  SOURCES
    transform_processor_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::transform_test_fixture
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
