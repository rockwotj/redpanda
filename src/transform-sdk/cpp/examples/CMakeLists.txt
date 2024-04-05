cmake_minimum_required(VERSION 3.22)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(
  RedpandaDataTransformExamples
  VERSION 0.1
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(
  redpanda-transform-sdk
  SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
)

FetchContent_Declare(
  json 
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)

FetchContent_MakeAvailable(redpanda-transform-sdk json)

add_compile_definitions(EI_PORTING_POSIX=1 CLOCK_PROCESS_CPUTIME_ID=CLOCK_MONOTONIC)

add_executable(
  app
  app.cc
)
target_link_libraries(
  app
  Redpanda::transform_sdk
  nlohmann_json::nlohmann_json
)

add_subdirectory(edge-impulse-export)

