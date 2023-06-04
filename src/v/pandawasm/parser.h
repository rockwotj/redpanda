#pragma once

#include "bytes/iobuf.h"
#include "pandawasm/ast.h"

namespace pandawasm {

class parse_exception : public std::exception {
public:
    explicit parse_exception(std::string msg)
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    std::string _msg;
};
class module_too_large_exception : public parse_exception {
public:
    explicit module_too_large_exception(std::string msg)
      : parse_exception(std::move(msg)) {}
};

ss::future<parsed_module> parse_module(iobuf);

} // namespace pandawasm
