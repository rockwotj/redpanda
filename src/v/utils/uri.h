#pragma once

#include "seastarx.h"
#include <seastar/core/sstring.hh>
#include <optional>
#include <string_view>

namespace util {

struct uri {
  ss::sstring scheme;
  ss::sstring host;
  ss::sstring port;
  ss::sstring path;
  ss::sstring query;
};

std::optional<uri> parse_uri(std::string_view);
} // namespace util
