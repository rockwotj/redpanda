v_cc_library(
  NAME pandawasm
  SRCS
    parser.cc
  HDRS
    encoding.h
  DEPS
    Seastar::seastar
    v::utils
)

add_subdirectory(tests)
