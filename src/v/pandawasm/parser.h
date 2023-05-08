#pragma once

#include "bytes/iobuf.h"

namespace pandawasm {

class parse_exception : public std::exception {};

ss::future<> parse_module(iobuf);

}
