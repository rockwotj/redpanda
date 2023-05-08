#include "pandawasm/parser.h"

#include "bytes/iobuf_parser.h"
#include "pandawasm/encoding.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <cstdint>
#include <vector>

namespace pandawasm {

namespace {

template<typename Type, ss::future<Type> (*parse_type)(iobuf_const_parser&)>
ss::future<std::vector<Type>> parse_section(iobuf_const_parser& parser) {
    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    std::vector<Type> vector;
    for (uint32_t i = 0; i < vector_size; ++i) {
        vector.push_back(co_await parse_type(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return vector;
}
template<typename Type, Type (*parse_type)(iobuf_const_parser&)>
ss::future<std::vector<Type>> parse_section(iobuf_const_parser& parser) {
    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    std::vector<Type> vector;
    for (uint32_t i = 0; i < vector_size; ++i) {
        vector.push_back(parse_type(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return vector;
}

enum class valtype : uint8_t {
    i32 = 0x7F,
    i64 = 0x7E,
    f32 = 0x7D,
    f64 = 0x7C,
    v128 = 0x7B,
    funcref = 0x70,
    externref = 0x6F,
};

struct function_type {
    std::vector<valtype> parameter_types;
    std::vector<valtype> result_types;
};

valtype parse_valtype(iobuf_const_parser& parser) {
    auto type_id = parser.consume_le_type<uint8_t>();
    switch (type_id) {
    case uint8_t(valtype::i32):
    case uint8_t(valtype::i64):
    case uint8_t(valtype::f32):
    case uint8_t(valtype::f64):
    case uint8_t(valtype::funcref):
    case uint8_t(valtype::externref):
        return valtype(type_id);
    default:
        throw parse_exception();
    }
}

ss::future<std::vector<valtype>> parse_result_type(iobuf_const_parser& parser) {
    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    std::vector<valtype> result_type;
    result_type.reserve(vector_size);
    for (uint32_t i = 0; i < vector_size; ++i) {
        result_type.push_back(parse_valtype(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return result_type;
}

ss::future<function_type> parse_function_type(iobuf_const_parser& parser) {
    auto magic = parser.consume_le_type<uint8_t>();
    if (magic != 0x60) {
        throw parse_exception();
    }
    auto parameter_types = co_await parse_result_type(parser);
    auto result_types = co_await parse_result_type(parser);
    co_return function_type{
      .parameter_types = std::move(parameter_types),
      .result_types = std::move(result_types)};
}

struct limits {
    uint32_t min;
    uint32_t max; // Empty maximums use numeric_limits::max
};
limits parse_limits(iobuf_const_parser& parser) {
    if (parser.read_bool()) {
        auto min = encoding::decode_leb128<uint32_t>(parser);
        auto max = encoding::decode_leb128<uint32_t>(parser);
        return {.min = min, .max = max};
    } else {
        auto min = encoding::decode_leb128<uint32_t>(parser);
        return {.min = min, .max = std::numeric_limits<uint32_t>::max()};
    }
}

using name = named_type<ss::sstring, struct name_tag>;

name parse_name(iobuf_const_parser& parser) {
    auto str_len = encoding::decode_leb128<uint32_t>(parser);
    auto str = parser.read_string(str_len);
    // TODO: validate utf8
    return name(str);
}

using typeidx = named_type<uint32_t, struct typeidx_tag>;

typeidx parse_typeidx(iobuf_const_parser& parser) {
    return typeidx(encoding::decode_leb128<uint32_t>(parser));
}

struct tabletype {
    limits limits;
    valtype reftype; // funcref | externref
};

tabletype parse_tabletype(iobuf_const_parser& parser) {
    auto reftype = parse_valtype(parser);
    if (reftype != valtype::externref && reftype != valtype::funcref) {
        throw parse_exception();
    }
    auto limits = parse_limits(parser);
    return {.limits = limits, .reftype = reftype};
}

struct memtype {
    limits limits;
};

memtype parse_memtype(iobuf_const_parser& parser) {
    return {.limits = parse_limits(parser)};
}

struct globaltype {
    valtype valtype;
    bool mut;
};

globaltype parse_globaltype(iobuf_const_parser& parser) {
    auto valtype = parse_valtype(parser);
    auto mut = parser.read_bool();
    return {.valtype = valtype, .mut = mut};
}

struct module_import {
    using desc = std::variant<typeidx, tabletype, memtype, globaltype>;
    ss::sstring module;
    ss::sstring name;
    desc description;
};

module_import parse_import(iobuf_const_parser& parser) {
    auto module = parse_name(parser);
    auto name = parse_name(parser);
    auto type = parser.consume_le_type<uint8_t>();
    module_import::desc desc;
    switch (type) {
    case 0x00: // func
        desc = parse_typeidx(parser);
        break;
    case 0x01: // table
        desc = parse_tabletype(parser);
        break;
    case 0x02: // memory
        desc = parse_memtype(parser);
        break;
    case 0x03: // global
        desc = parse_globaltype(parser);
        break;
    default:
        throw parse_exception();
    }
    return {
      .module = std::move(module),
      .name = std::move(name),
      .description = desc};
}

struct table {
    tabletype type;
};

table parse_table(iobuf_const_parser& parser) {
    return {.type = parse_tabletype(parser)};
}

struct mem {
    memtype type;
};

mem parse_memory(iobuf_const_parser& parser) {
    return {.type = parse_memtype(parser)};
}

struct expression {
    iobuf instructions;
};

struct global {
    globaltype type;
    expression expr;
};

ss::future<> parse_one_section(iobuf_const_parser& parser) {
    auto id = parser.consume_le_type<uint8_t>();
    auto size = encoding::decode_leb128<uint32_t>(parser);
    switch (id) {
    case 0x00: // Custom section
        // Skip over custom sections for now
        // TODO: Support debug symbols if included.
        parser.skip(size);
        co_return;
    case 0x01: // type section
        co_await parse_section<function_type, parse_function_type>(parser);
        co_return;
    case 0x02: // import section
        co_await parse_section<module_import, parse_import>(parser);
        co_return;
    case 0x03: // function section
        co_await parse_section<typeidx, parse_typeidx>(parser);
        co_return;
    case 0x04: // table section
        co_await parse_section<table, parse_table>(parser);
        co_return;
    case 0x05: // memory section
        co_await parse_section<mem, parse_memory>(parser);
        co_return;
    case 0x06: // global section
    case 0x07: // export section
    case 0x08: // start section
    case 0x09: // element section
    case 0x0A: // code section
    case 0x0B: // data section
    case 0x0C: // data count section
        break;
    default:
        throw parse_exception();
    }
}

} // namespace

ss::future<> parse_module(iobuf buffer) {
    iobuf_const_parser parser(buffer);
    bytes magic = parser.read_bytes(4);
    if (magic != "\0asm") {
        throw parse_exception();
    }
    auto version = parser.consume_le_type<int32_t>();
    if (version != 1) {
        throw parse_exception();
    }

    while (parser.bytes_left() > 0) {
        co_await parse_one_section(parser);
        co_await ss::coroutine::maybe_yield();
    }
}

} // namespace pandawasm
