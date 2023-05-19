#pragma once

#include "bytes/iobuf.h"

namespace pandawasm {

class parse_exception : public std::exception {};
class module_too_large_exception : public parse_exception {};

ss::future<> parse_module(iobuf);

}
