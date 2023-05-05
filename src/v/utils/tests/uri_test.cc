// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/uri.h"

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
  
BOOST_AUTO_TEST_CASE(parse_uri) {
  std::string uri = "http://localhost:7777/path?query=yes";
  auto parsed = util::parse_uri(uri);
  BOOST_CHECK(parsed.has_value());
  BOOST_CHECK_EQUAL(parsed->scheme, "http");
  BOOST_CHECK_EQUAL(parsed->host, "localhost");
  BOOST_CHECK_EQUAL(parsed->port, "7777");
  BOOST_CHECK_EQUAL(parsed->path, "/path");
  BOOST_CHECK_EQUAL(parsed->query, "query=yes");
}

BOOST_AUTO_TEST_CASE(parse_uri_without_port_or_query) {
  std::string uri = "https://example.com/path/sub";
  auto parsed = util::parse_uri(uri);
  BOOST_CHECK(parsed.has_value());
  BOOST_CHECK_EQUAL(parsed->scheme, "https");
  BOOST_CHECK_EQUAL(parsed->host, "example.com");
  BOOST_CHECK_EQUAL(parsed->port, "");
  BOOST_CHECK_EQUAL(parsed->path, "/path/sub");
  BOOST_CHECK_EQUAL(parsed->query, "");
}
