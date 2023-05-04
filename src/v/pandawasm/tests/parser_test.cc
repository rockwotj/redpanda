
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(parse_simple_module) {
  BOOST_CHECK_EQUAL(1, 1);
}
