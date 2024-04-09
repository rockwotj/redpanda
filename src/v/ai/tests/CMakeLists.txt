add_executable(ai_demo demo.cc)
target_link_libraries(ai_demo PUBLIC v::ai absl::strings)
set_property(TARGET ai_demo PROPERTY POSITION_INDEPENDENT_CODE ON)
