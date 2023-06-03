v_cc_library(
  NAME pandawasm
  SRCS
    parser.cc
  HDRS
    encoding.h
  DEPS
    asmjit
    Seastar::seastar
    v::utils
)

add_subdirectory(tests)
