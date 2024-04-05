# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/.."
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-build"
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix"
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/tmp"
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/src/redpanda-transform-sdk-populate-stamp"
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/src"
  "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/src/redpanda-transform-sdk-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/src/redpanda-transform-sdk-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/rockwood/code/transform-sdk/src/transform-sdk/cpp/examples/lb/_deps/redpanda-transform-sdk-subbuild/redpanda-transform-sdk-populate-prefix/src/redpanda-transform-sdk-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
