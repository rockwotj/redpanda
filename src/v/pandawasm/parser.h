#pragma once

#include "bytes/iobuf.h"
#include "pandawasm/ast.h"

namespace pandawasm {

class parse_exception : public std::exception {};
class module_too_large_exception : public parse_exception {};

ss::future<parsed_module> parse_module(iobuf);

} // namespace pandawasm
