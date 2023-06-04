
#pragma once

#include "instruction.h"
#include "seastarx.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"
#include "value.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <variant>
#include <vector>

namespace pandawasm {

struct function_type {
    std::vector<valtype> parameter_types;
    std::vector<valtype> result_types;
};

struct limits {
    uint32_t min;
    uint32_t max; // Empty maximums use numeric_limits::max
};

using name = named_type<ss::sstring, struct name_tag>;
using typeidx = named_type<uint32_t, struct typeidx_tag>;
using funcidx = named_type<uint32_t, struct funcidx_tag>;
using localidx = named_type<uint32_t, struct funcidx_tag>;
struct tabletype {
    limits limits;
    valtype reftype; // funcref | externref
};

struct memtype {
    limits limits;
};

struct globaltype {
    valtype valtype;
    bool mut;
};

struct module_import {
    using desc = std::variant<typeidx, tabletype, memtype, globaltype>;
    ss::sstring module;
    ss::sstring name;
    desc description;
};

struct table {
    tabletype type;
};

struct mem {
    memtype type;
};

struct global {
    globaltype type;
    value value;
};

struct module_export {
    using desc = std::variant<typeidx, tabletype, memtype, globaltype>;
    ss::sstring name;
    desc description;
};

struct function {
    function_type type;
    std::vector<valtype> locals;
    std::vector<instruction> body;
};

struct parsed_module {
    fragmented_vector<function> functions;
};
} // namespace pandawasm
