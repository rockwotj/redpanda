v_cc_library(
  NAME pandawasm
  SRCS
    instruction.cc
    parser.cc
    value.cc
    compiler.cc
    validator.cc
  HDRS
    encoding.h
  DEPS
    asmjit
    Seastar::seastar
    v::utils
)

add_subdirectory(tests)
