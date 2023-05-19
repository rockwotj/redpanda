corrosion_import_crate(MANIFEST_PATH Cargo.toml)

corrosion_add_cxxbridge(wasmstar-bridge CRATE wasmstar MANIFEST_PATH . FILES lib.rs)
target_include_directories(wasmstar-bridge PUBLIC src)
target_sources(wasmstar-bridge PRIVATE 
  src/ffi.cc
)
target_link_libraries(wasmstar-bridge PUBLIC Seastar::seastar)
