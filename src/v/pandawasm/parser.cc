#include "pandawasm/parser.h"

#include "bytes/iobuf_parser.h"
#include "pandawasm/ast.h"
#include "pandawasm/encoding.h"
#include "pandawasm/instruction.h"
#include "seastarx.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <cstdint>
#include <iterator>
#include <vector>

namespace pandawasm {

namespace {

template<typename Type, ss::future<Type> (*parse_type)(iobuf_const_parser&)>
ss::future<fragmented_vector<Type>> parse_section(iobuf_const_parser& parser) {
    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    fragmented_vector<Type> vector;
    for (uint32_t i = 0; i < vector_size; ++i) {
        vector.push_back(co_await parse_type(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return vector;
}
template<typename Type, Type (*parse_type)(iobuf_const_parser&)>
ss::future<fragmented_vector<Type>> parse_section(iobuf_const_parser& parser) {
    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    fragmented_vector<Type> vector;
    for (uint32_t i = 0; i < vector_size; ++i) {
        vector.push_back(parse_type(parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return vector;
}

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

name parse_name(iobuf_const_parser& parser) {
    auto str_len = encoding::decode_leb128<uint32_t>(parser);
    auto str = parser.read_string(str_len);
    // TODO: validate utf8
    return name(str);
}

typeidx parse_typeidx(iobuf_const_parser& parser) {
    return typeidx(encoding::decode_leb128<uint32_t>(parser));
}

funcidx parse_funcidx(iobuf_const_parser& parser) {
    return funcidx(encoding::decode_leb128<uint32_t>(parser));
}

tabletype parse_tabletype(iobuf_const_parser& parser) {
    auto reftype = parse_valtype(parser);
    if (reftype != valtype::externref && reftype != valtype::funcref) {
        throw parse_exception();
    }
    auto limits = parse_limits(parser);
    return {.limits = limits, .reftype = reftype};
}

memtype parse_memtype(iobuf_const_parser& parser) {
    return {.limits = parse_limits(parser)};
}

globaltype parse_globaltype(iobuf_const_parser& parser) {
    auto valtype = parse_valtype(parser);
    auto mut = parser.read_bool();
    return {.valtype = valtype, .mut = mut};
}

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

table parse_table(iobuf_const_parser& parser) {
    return {.type = parse_tabletype(parser)};
}

mem parse_memory(iobuf_const_parser& parser) {
    return {.type = parse_memtype(parser)};
}

value parse_const_expr(iobuf_const_parser& parser) {
    auto opcode = parser.consume_le_type<uint8_t>();
    switch (opcode) {
    case 0x41:
        return value{.i32 = encoding::decode_leb128<uint32_t>(parser)};
    case 0x42:
        return value{.i64 = encoding::decode_leb128<uint64_t>(parser)};
    case 0x43:
        return value{.f32 = parser.consume_type<float>()};
    case 0x44:
        return value{.f64 = parser.consume_type<float>()};
    default:
        // TODO: Support refs, other global references, and vectors
        throw parse_exception();
    }
}

global parse_global(iobuf_const_parser& parser) {
    auto type = parse_globaltype(parser);
    auto value = parse_const_expr(parser);
    return {.type = type, .value = value};
}

module_export parse_export(iobuf_const_parser& parser) {
    auto name = parse_name(parser);
    auto type = parser.consume_le_type<uint8_t>();
    module_export::desc desc;
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
    return {.name = std::move(name), .description = desc};
}

constexpr size_t MAX_FUNCTION_LOCALS = 256;

std::vector<instruction> parse_expression(iobuf_const_parser& parser) {
    // TODO: Validate that operators are valid to perform
    std::vector<instruction> instruction_vector;
    // Ensure this isn't going to be too large
    for (auto opcode = parser.consume_le_type<uint8_t>(); opcode != 0x0B;
         opcode = parser.consume_le_type<uint8_t>()) {
        switch (opcode) {
        case 0x01:
            instruction_vector.push_back(op(instructions::noop));
            break;
        case 0x0F:
            instruction_vector.push_back(op(instructions::retrn));
            break;
        case 0x20:
            instruction_vector.push_back(op(instructions::get_local_i32));
            instruction_vector.push_back(
              val({.i32 = encoding::decode_leb128<uint32_t>(parser)}));
            break;
        case 0x21:
            instruction_vector.push_back(op(instructions::set_local_i32));
            instruction_vector.push_back(
              val({.i32 = encoding::decode_leb128<uint32_t>(parser)}));
            break;
        case 0x41:
            instruction_vector.push_back(op(instructions::const_i32));
            instruction_vector.push_back(
              val({.i32 = encoding::decode_leb128<uint32_t>(parser)}));
            break;
        case 0x6A:
            instruction_vector.push_back(op(instructions::add_i32));
            break;
        default:
            throw parse_exception();
        }
    }
    return instruction_vector;
}

code parse_code(iobuf_const_parser& parser) {
    auto expected_size = encoding::decode_leb128<uint32_t>(parser);
    auto start_position = parser.bytes_consumed();

    auto vector_size = encoding::decode_leb128<uint32_t>(parser);
    std::vector<valtype> locals;
    for (uint32_t i = 0; i < vector_size; ++i) {
        auto num_locals = encoding::decode_leb128<uint32_t>(parser);
        if (num_locals + locals.size() > MAX_FUNCTION_LOCALS) {
            throw module_too_large_exception();
        }
        auto valtype = parse_valtype(parser);
        std::fill_n(std::back_inserter(locals), num_locals, valtype);
    }
    auto body = parse_expression(parser);
    auto actual = parser.bytes_consumed() - start_position;

    if (actual != expected_size) {
        throw parse_exception();
    }
    return {.locals = locals, .body = std::move(body)};
}

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
        co_await parse_section<global, parse_global>(parser);
        co_return;
    case 0x07: // export section
        co_await parse_section<module_export, parse_export>(parser);
        co_return;
    case 0x08: // start section
        parse_funcidx(parser);
        co_return;
    case 0x09: // element section
        // TODO: Implement me
        throw parse_exception();
    case 0x0A: // code section
        co_await parse_section<code, parse_code>(parser);
        co_return;
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
