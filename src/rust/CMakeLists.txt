find_package(Corrosion REQUIRED)

# Importing as a cdylib seems to drop symbols somehow, just use the static library for now.
corrosion_import_crate(
  MANIFEST_PATH Cargo.toml
  CRATE_TYPES staticlib
)

# TODO: We should be setting all of our "normal" flags here too
corrosion_set_env_vars(wasmstar CXXSTDLIB=c++ CXXFLAGS=-stdlib=libc++)

set(RUST_BRIDGE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/lib.rs)
set(CXX_GENERATED_HDR ${CMAKE_CURRENT_BINARY_DIR}/wasmstar.h)
set(CXX_HDR ${CMAKE_CURRENT_BINARY_DIR}/cxx.h)
set(CXX_GENERATED_SRC ${CMAKE_CURRENT_BINARY_DIR}/wasmstar.cc)
set(FFI_HDR ${CMAKE_CURRENT_BINARY_DIR}/ffi.h)
set(FFI_SRC ${CMAKE_CURRENT_BINARY_DIR}/ffi.cc)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/ffi.h ${FFI_HDR} COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/ffi.cc ${FFI_SRC} COPYONLY)

add_custom_command(
        OUTPUT ${CXX_GENERATED_HDR}
        COMMAND ${CXXBRIDGE} ${RUST_BRIDGE_FILE} --header > ${CXX_GENERATED_HDR}
        DEPENDS ${RUST_BRIDGE_FILE}
        COMMENT "Generating cxxbridge header"
)

add_custom_command(
        OUTPUT ${CXX_HDR}
        COMMAND ${CXXBRIDGE} --header > ${CXX_HDR}
        COMMENT "Generating cxx header"
)

add_custom_command(
        OUTPUT ${CXX_GENERATED_SRC}
        COMMAND ${CXXBRIDGE} ${RUST_BRIDGE_FILE} > ${CXX_GENERATED_SRC}
        DEPENDS ${RUST_BRIDGE_FILE}
        COMMENT "Generating cxxbridge source"
)

v_cc_library(
  NAME wasmstar
  HDRS
    ${FFI_HDR}
    ${CXX_GENERATED_HDR}
    ${CXX_HDR}
  SRCS
    ${FFI_SRC}
    ${CXX_GENERATED_SRC}
  DEPS
    Seastar::seastar
    wasmstar
  COPTS
    -I${CMAKE_CURRENT_BINARY_DIR}
)
