
rp_test(
    UNIT_TEST
    BINARY_NAME parser_test
    SOURCES parser_test.cc
    LIBRARIES v::seastar_testing_main v::pandawasm
    LABELS pandawasm
    ARGS "-- -c 1"
)
