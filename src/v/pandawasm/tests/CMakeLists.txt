
rp_test(
    UNIT_TEST
    BINARY_NAME parser_test
    SOURCES parser_test.cc
    LIBRARIES v::seastar_testing_main v::pandawasm
    LABELS pandawasm
    ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME encoding_unit
  SOURCES
    encoding_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::pandawasm
  LABELS pandawasm
)

rp_test(
    UNIT_TEST
    BINARY_NAME validator_test
    SOURCES validator_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::pandawasm
    LABELS pandawasm
    ARGS "-- -c 1"
)
