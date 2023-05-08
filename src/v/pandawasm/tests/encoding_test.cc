// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <limits>
#include <vector>
#define BOOST_TEST_MODULE leb128

#include "bytes/bytes.h"
#include "pandawasm/encoding.h"

#include <boost/test/unit_test.hpp>

template<typename int_type>
struct testcase {
    int_type decoded;
    bytes encoded;
};

using pandawasm::encoding::decode_exception;
using pandawasm::encoding::decode_leb128;
using pandawasm::encoding::encode_leb128;

template<typename int_type>
void run_testcase(const testcase<int_type>& testcase) {
    BOOST_TEST_INFO("encoding: " << testcase.decoded);
    BOOST_CHECK_EQUAL(
      encode_leb128<int_type>(testcase.decoded), testcase.encoded);

    BOOST_TEST_INFO("decoding: " << testcase.decoded);
    auto iobuf = bytes_to_iobuf(testcase.encoded);
    auto parser = iobuf_const_parser(iobuf);
    BOOST_CHECK_EQUAL(decode_leb128<int_type>(parser), testcase.decoded);
}

BOOST_AUTO_TEST_CASE(leb128_int32) {
    std::vector<testcase<int32_t>> testcases(
      {{.decoded = -165675008, .encoded = {0x80, 0x80, 0x80, 0xb1, 0x7f}},
       {.decoded = -624485, .encoded = {0x9b, 0xf1, 0x59}},
       {.decoded = -16256, .encoded = {0x80, 0x81, 0x7f}},
       {.decoded = -4, .encoded = {0x7c}},
       {.decoded = -1, .encoded = {0x7f}},
       {.decoded = 0, .encoded = {0x00}},
       {.decoded = 1, .encoded = {0x01}},
       {.decoded = 4, .encoded = {0x04}},
       {.decoded = 16256, .encoded = {0x80, 0xff, 0x0}},
       {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
       {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0xcf, 0x0}},
       {.decoded = std::numeric_limits<int32_t>::max(),
        .encoded = {0xff, 0xff, 0xff, 0xff, 0x7}}});
    for (const auto& testcase : testcases) {
        run_testcase(testcase);
    }
}

BOOST_AUTO_TEST_CASE(leb128_uint32) {
    std::vector<testcase<uint32_t>> testcases(
      {{.decoded = 0, .encoded = {0x00}},
       {.decoded = 1, .encoded = {0x01}},
       {.decoded = 4, .encoded = {0x04}},
       {.decoded = 16256, .encoded = {0x80, 0x7f}},
       {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
       {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0x4f}},
       {.decoded = std::numeric_limits<uint32_t>::max(),
        .encoded = {0xff, 0xff, 0xff, 0xff, 0xf}}});
    for (const auto& testcase : testcases) {
        run_testcase(testcase);
    }
}

BOOST_AUTO_TEST_CASE(leb128_uint64) {
    std::vector<testcase<uint64_t>> testcases(
      {{.decoded = 0, .encoded = {0x00}},
       {.decoded = 1, .encoded = {0x01}},
       {.decoded = 4, .encoded = {0x04}},
       {.decoded = 16256, .encoded = {0x80, 0x7f}},
       {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
       {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0x4f}},
       {.decoded = std::numeric_limits<uint32_t>::max(),
        .encoded = {0xff, 0xff, 0xff, 0xff, 0xf}},
       {.decoded = std::numeric_limits<uint64_t>::max(),
        .encoded = {
          0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1}}});
    for (const auto& testcase : testcases) {
        run_testcase(testcase);
    }
}

BOOST_AUTO_TEST_CASE(leb128_int64) {
    std::vector<testcase<int64_t>> testcases({
      {.decoded = -std::numeric_limits<int32_t>::max(),
       .encoded = {0x81, 0x80, 0x80, 0x80, 0x78}},
      {.decoded = -165675008, .encoded = {0x80, 0x80, 0x80, 0xb1, 0x7f}},
      {.decoded = -624485, .encoded = {0x9b, 0xf1, 0x59}},
      {.decoded = -16256, .encoded = {0x80, 0x81, 0x7f}},
      {.decoded = -4, .encoded = {0x7c}},
      {.decoded = -1, .encoded = {0x7f}},
      {.decoded = 0, .encoded = {0x00}},
      {.decoded = 1, .encoded = {0x01}},
      {.decoded = 4, .encoded = {0x04}},
      {.decoded = 16256, .encoded = {0x80, 0xff, 0x0}},
      {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
      {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0xcf, 0x0}},
      {.decoded = std::numeric_limits<int32_t>::max(),
       .encoded = {0xff, 0xff, 0xff, 0xff, 0x7}},
      {.decoded = std::numeric_limits<int64_t>::max(),
       .encoded = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0}},
    });
    for (const auto& testcase : testcases) {
        run_testcase(testcase);
    }
}

BOOST_AUTO_TEST_CASE(overflow_64) {
    bytes encoded(size_t(11), 0xff);
    auto iobuf = bytes_to_iobuf(encoded);
    auto parser = iobuf_const_parser(iobuf);
    BOOST_CHECK_THROW(decode_leb128<int64_t>(parser), decode_exception);
    parser = iobuf_const_parser(iobuf);
    BOOST_CHECK_THROW(decode_leb128<int64_t>(parser), decode_exception);
}

BOOST_AUTO_TEST_CASE(overflow_32) {
    bytes encoded = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    auto iobuf = bytes_to_iobuf(encoded);
    auto parser = iobuf_const_parser(iobuf);
    BOOST_CHECK_THROW(decode_leb128<int32_t>(parser), decode_exception);
    parser = iobuf_const_parser(iobuf);
    BOOST_CHECK_THROW(decode_leb128<uint32_t>(parser), decode_exception);
}
