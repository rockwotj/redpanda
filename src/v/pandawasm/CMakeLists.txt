v_cc_library(
  NAME pandawasm
  SRCS
    parser.cc
  DEPS
    Seastar::seastar
    v::utils
)

add_subdirectory(tests)
