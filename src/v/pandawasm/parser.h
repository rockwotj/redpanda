#pragma once

#include "bytes/iobuf.h"

namespace pandawasm {

ss::future<> parse_module(iobuf);

}
