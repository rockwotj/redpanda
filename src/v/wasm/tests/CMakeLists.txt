v_cc_library(
  NAME wasm_test_fixture
  HDRS
    wasm_fixture.h
  SRCS
    "wasm_fixture.cc"
  DEPS
    v::wasm 
    Seastar::seastar
    v::model_test_utils
)

rp_test(
  UNIT_TEST
  BINARY_NAME wasm_transform_test
  SOURCES
    wasm_transform_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm_test_fixture
  # TODO: Hookup this to the build system properly
  INPUT_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/testdata/golang_identity_transform.wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  BINARY_NAME wasm_transform_error_test
  SOURCES
    wasm_transform_error_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm_test_fixture
  # TODO: Hookup this to the build system properly
  INPUT_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/testdata/transform_error.wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  BINARY_NAME wasm_transform_trap_test
  SOURCES
    wasm_transform_trap_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm_test_fixture
  # TODO: Hookup this to the build system properly
  INPUT_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/testdata/transform_panic.wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  BINARY_NAME wasm_setup_trap_test
  SOURCES
    wasm_setup_trap_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm_test_fixture
  # TODO: Hookup this to the build system properly
  INPUT_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/testdata/setup_panic.wasm
  ARGS "-- -c 1"
  LABELS wasm
)
