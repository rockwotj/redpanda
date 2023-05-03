
rp_test(
  UNIT_TEST
  BINARY_NAME wasm_transform_test
  SOURCES
    wasm_transform_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm 
    v::model_test_utils
  # TODO: Hookup this to the build system properly
  INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/testdata/identity_transform.wasm
  ARGS "-- -c 1"
  LABELS wasm
)
