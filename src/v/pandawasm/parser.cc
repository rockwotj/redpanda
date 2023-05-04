
#include "bytes/iobuf_parser.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <cstdint>
#include <vector>

namespace pandawasm {
namespace {

class parse_exception : public std::exception {};

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
    auto vector_size = parser.consume_le_type<uint32_t>();
    std::vector<valtype> result_type;
    result_type.reserve(vector_size);
    for (uint32_t i = 0; i < vector_size; ++i) {
        result_type.push_back(parse_valtype(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return result_type;
}

ss::future<std::vector<function_type>>
parse_type_section(iobuf_const_parser& parser) {
    auto vector_size = parser.consume_le_type<uint32_t>();
    std::vector<function_type> function_types;
    for (uint32_t i = 0; i < vector_size; ++i) {
        auto magic = parser.consume_le_type<uint8_t>();
        if (magic != 0x60) {
            throw parse_exception();
        }
        auto parameter_types = co_await parse_result_type(parser);
        auto result_types = co_await parse_result_type(parser);
        function_types.emplace_back(parameter_types, result_types);
        co_await ss::coroutine::maybe_yield();
    }
    co_return function_types;
}

struct limits {
    uint32_t min;
    uint32_t max; // Empty maximums use numeric_limits::max
};
limits parse_limits(iobuf_const_parser& parser) {
  if (parser.read_bool()) {
    auto min = parser.consume_le_type<uint32_t>();
    auto max = parser.consume_le_type<uint32_t>();
    return {.min = min, .max = max};
  } else {
    auto min = parser.consume_le_type<uint32_t>();
    return {.min = min, .max = std::numeric_limits<uint32_t>::max()};
  }
}

using name = named_type<ss::sstring, struct name_tag>;

name parse_name(iobuf_const_parser& parser) {
  auto str_len = parser.consume_le_type<uint32_t>();
  auto str = parser.read_string(str_len);
  // TODO: validate utf8
  return name(str);
}

using typeidx = named_type<uint32_t, struct typeidx_tag>;

typeidx parse_typeidx(iobuf_const_parser& parser) {
  return typeidx(parser.consume_le_type<uint32_t>());
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

struct import {
    using desc = std::variant<typeidx, tabletype, memtype, globaltype>;
    ss::sstring module;
    ss::sstring name;
    desc description;
};

import parse_import(iobuf_const_parser& parser) {
  auto module = parse_name(parser);
  auto name = parse_name(parser);
  auto type = parser.consume_le_type<uint8_t>();
  import::desc desc;
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
  return {.module = std::move(module), .name = std::move(name), .description = desc};
}

ss::future<std::vector<import>> parse_import_section(iobuf_const_parser& parser) {
    auto vector_size = parser.consume_le_type<uint32_t>();
    std::vector<import> imports;
    imports.reserve(vector_size);
    for (uint32_t i = 0; i < vector_size; ++i) {
        imports.push_back(parse_import(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return imports;
}

ss::future<> parse_section(iobuf_const_parser& parser) {
    auto id = parser.consume_le_type<uint8_t>();
    auto size = parser.consume_le_type<uint32_t>();
    switch (id) {
    case 0x00: // Custom section
        // Skip over custom sections for now
        parser.skip(size);
        co_return;
    case 0x01: // type section
        co_await parse_type_section(parser);
    case 0x02: // import section
        co_await parse_import_section(parser);
    case 0x03: // function section
    case 0x04: // table section
    case 0x05: // memory section
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
        co_await parse_section(parser);
        co_await ss::coroutine::maybe_yield();
    }
}

} // namespace pandawasm
