#include "pandawasm/parser.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "pandawasm/ast.h"
#include "pandawasm/encoding.h"
#include "pandawasm/instruction.h"
#include "pandawasm/validator.h"
#include "seastarx.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"
#include "utils/utf8.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <asmjit/core/globals.h>

#include <cstdint>
#include <iterator>
#include <utility>
#include <vector>

namespace pandawasm {

namespace {

constexpr size_t MAX_FUNCTIONS = 1U << 16U;
constexpr size_t MAX_FUNCTION_LOCALS = 1U << 8U;
constexpr size_t MAX_FUNCTION_PARAMETERS = asmjit::Globals::kMaxFuncArgs;
constexpr size_t MAX_FUNCTION_SIGNATURES = 1U << 17U;
constexpr size_t MAX_IMPORTS = 1U << 8U;
// NOTE: Tables and memories both have maximums for an individual entity, but
// those are validated at runtime.
constexpr size_t MAX_TABLES = 1U << 4U;
constexpr size_t MAX_MEMORIES = 1;
constexpr size_t MAX_GLOBALS = 1U << 10U;
constexpr size_t MAX_EXPORTS = 1U << 8U;

valtype parse_valtype(iobuf_const_parser* parser) {
    auto type_id = parser->consume_le_type<uint8_t>();
    switch (type_id) {
    case uint8_t(valtype::i32):
    case uint8_t(valtype::i64):
    case uint8_t(valtype::f32):
    case uint8_t(valtype::f64):
    case uint8_t(valtype::funcref):
    case uint8_t(valtype::externref):
        return valtype(type_id);
    default:
        throw parse_exception(ss::format("unknown valtype: {}", type_id));
    }
}

template<typename NamedInteger, typename Vector>
void validate_in_range(
  std::string_view msg, NamedInteger idx, const Vector& v) {
    if (idx() < 0 || idx() >= v.size()) {
        throw parse_exception(
          ss::format("{} out of range - {} ∉ [0, {})", msg, idx, v.size()));
    }
}

name parse_name(iobuf_const_parser* parser) {
    auto str_len = leb128::decode<uint32_t>(parser);
    auto str = parser->read_string(str_len);
    struct utf8_thrower {
        [[noreturn]] [[gnu::cold]] void conversion_error() {
            throw parse_exception("Cannot decode string as UTF8");
        }
    };
    validate_utf8(str, utf8_thrower{});
    return name(str);
}

typeidx parse_typeidx(iobuf_const_parser* parser) {
    return typeidx(leb128::decode<uint32_t>(parser));
}

funcidx parse_funcidx(iobuf_const_parser* parser) {
    return funcidx(leb128::decode<uint32_t>(parser));
}
tableidx parse_tableidx(iobuf_const_parser* parser) {
    return tableidx(leb128::decode<uint32_t>(parser));
}
memidx parse_memidx(iobuf_const_parser* parser) {
    return memidx(leb128::decode<uint32_t>(parser));
}
globalidx parse_globalidx(iobuf_const_parser* parser) {
    return globalidx(leb128::decode<uint32_t>(parser));
}

std::vector<valtype> parse_signature_types(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_FUNCTION_PARAMETERS) {
        throw module_too_large_exception(
          ss::format("too many parameters to function: {}", vector_size));
    }
    std::vector<valtype> result_type;
    result_type.reserve(vector_size);
    for (uint32_t i = 0; i < vector_size; ++i) {
        result_type.push_back(parse_valtype(parser));
    }
    return result_type;
}

function_signature parse_signature(iobuf_const_parser* parser) {
    auto magic = parser->consume_le_type<uint8_t>();
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    if (magic != 0x60) {
        throw parse_exception(
          ss::format("function type magic mismatch: {}", magic));
    }
    auto parameter_types = parse_signature_types(parser);
    auto result_types = parse_signature_types(parser);
    return function_signature{
      .parameter_types = std::move(parameter_types),
      .result_types = std::move(result_types)};
}

limits parse_limits(iobuf_const_parser* parser) {
    if (parser->read_bool()) {
        auto min = leb128::decode<uint32_t>(parser);
        auto max = leb128::decode<uint32_t>(parser);
        return {.min = min, .max = max};
    } else {
        auto min = leb128::decode<uint32_t>(parser);
        return {.min = min, .max = std::numeric_limits<uint32_t>::max()};
    }
}

tabletype parse_tabletype(iobuf_const_parser* parser) {
    auto reftype = parse_valtype(parser);
    if (reftype != valtype::externref && reftype != valtype::funcref) {
        throw parse_exception(
          ss::format("invalid tabletype type: {}", reftype));
    }
    auto limits = parse_limits(parser);
    return {.limits = limits, .reftype = reftype};
}

memtype parse_memtype(iobuf_const_parser* parser) {
    return {.limits = parse_limits(parser)};
}

globaltype parse_globaltype(iobuf_const_parser* parser) {
    auto valtype = parse_valtype(parser);
    auto mut = parser->read_bool();
    return {.valtype = valtype, .mut = mut};
}

struct code {
    std::vector<valtype> locals;
    std::vector<instruction> body;
};

template<typename Type, auto (*parse_type)(iobuf_const_parser*)>
ss::future<fragmented_vector<Type>> parse_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    fragmented_vector<Type> vector;
    for (uint32_t i = 0; i < vector_size; ++i) {
        vector.push_back(co_await ss::futurize_invoke(parse_type, parser));
        co_await ss::coroutine::maybe_yield();
    }
    co_return vector;
}

/**
 * module_builder is responible for parsing the binary representation of a WASM
 * module and also enforcing various limits we've enforced.
 *
 * It also validates modules are well formed, although not all of that is done
 * directly. More complex validation is delegated to other helper classes.
 *
 * This class has "one and done" usage.
 *
 * Spec: https://webassembly.github.io/spec/core/binary/index.html
 */
class module_builder {
public:
    module_builder() = default;
    module_builder(const module_builder&) = delete;
    module_builder& operator=(const module_builder&) = delete;
    module_builder(module_builder&&) = delete;
    module_builder& operator=(module_builder&&) = delete;
    ~module_builder() = default;

    ss::future<> parse(iobuf_const_parser* parser);

    ss::future<parsed_module> build() &&;

private:
    // The main loop of parsing functions.
    //
    // At a high level from the spec. The sections are as follows and parsed in
    // order:
    // - type signatures
    // - imports
    // - function forward declarations
    // - tables
    // - memories
    // - globals
    // - exports
    // - start function
    // - elements (initializer code for tables)
    // - code (the bodies of functions that were forward declared)
    // - data (initializer code for memories)
    //
    // Around any of these sections can be a custom section, which we
    // currently ignore.
    ss::future<> parse_one_section(iobuf_const_parser* parser);

    // Parses the first section, which is made up of function signatures.
    ss::future<> parse_signature_section(iobuf_const_parser*);

    // Parses the forward declarations of functions.
    ss::future<> parse_function_declaration_section(iobuf_const_parser*);

    // Parses the imports for this module.
    //
    // NOTE: this does not validate that the imports exist. That will need to be
    // done at a later phase (or maybe that should be inputs to this..?)
    module_import parse_one_import(iobuf_const_parser*);
    ss::future<> parse_import_section(iobuf_const_parser*);

    ss::future<> parse_table_section(iobuf_const_parser*);

    ss::future<> parse_memories_section(iobuf_const_parser*);

    ss::future<> parse_globals_section(iobuf_const_parser*);

    module_export parse_one_export(iobuf_const_parser*);
    ss::future<> parse_exports_section(iobuf_const_parser*);

    // Parse a function body
    void parse_one_code(iobuf_const_parser*, function*);
    ss::future<> parse_code_section(iobuf_const_parser*);

    // In order to properly be able to stream parsing of modules, we need to
    // ensure everything is created in the correct order. The spec enforces that
    // modules are in order to achieve this usecase. This keeps track of that
    // bit so we can correctly enforce wellformed modules.
    //
    // NOTE: Custom sections are allowed to be anywhere.
    size_t _latest_section_read = 0;

    fragmented_vector<function_signature> _func_signatures;
    fragmented_vector<module_import> _imports;
    fragmented_vector<function> _functions;
    fragmented_vector<table> _tables;
    fragmented_vector<mem> _memories;
    fragmented_vector<global> _globals;
    fragmented_vector<module_export> _exports;
    std::optional<funcidx> _start;
};

ss::future<parsed_module> module_builder::build() && {
    co_return parsed_module{.functions = std::move(_functions)};
}

ss::future<>
module_builder::parse_signature_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_FUNCTION_SIGNATURES) {
        throw module_too_large_exception(ss::format(
          "too large of type section: {}, max: {}",
          vector_size,
          MAX_FUNCTION_SIGNATURES));
    }
    for (uint32_t i = 0; i < vector_size; ++i) {
        _func_signatures.push_back(parse_signature(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

ss::future<>
module_builder::parse_function_declaration_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_FUNCTIONS) {
        throw module_too_large_exception(ss::format(
          "too many functions: {}, max: {}",
          vector_size,
          MAX_FUNCTION_SIGNATURES));
    }
    for (uint32_t i = 0; i < vector_size; ++i) {
        auto funcidx = parse_funcidx(parser);
        validate_in_range(
          "unknown function signature", funcidx, _func_signatures);
        _functions.push_back(
          {.meta = {
             .signature = _func_signatures[funcidx()],
           }});
        co_await ss::coroutine::maybe_yield();
    }
}

module_import module_builder::parse_one_import(iobuf_const_parser* parser) {
    auto module_name = parse_name(parser);
    auto name = parse_name(parser);
    auto type = parser->consume_le_type<uint8_t>();
    module_import::desc desc;
    switch (type) {
    case 0x00: { // func
        auto funcidx = parse_typeidx(parser);
        validate_in_range(
          "unknown import function signature", funcidx, _func_signatures);
        desc = funcidx;
        break;
    }
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
        throw parse_exception(ss::format("unknown import type: {}", type));
    }
    // TODO: Validate the import
    return {
      .module_name = std::move(module_name),
      .name = std::move(name),
      .description = desc};
}

ss::future<> module_builder::parse_import_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_IMPORTS) {
        throw module_too_large_exception(ss::format(
          "too many imports: {}, max: {}", vector_size, MAX_IMPORTS));
    }

    for (uint32_t i = 0; i < vector_size; ++i) {
        _imports.push_back(parse_one_import(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

table parse_table(iobuf_const_parser* parser) {
    return {.type = parse_tabletype(parser)};
}

ss::future<> module_builder::parse_table_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_TABLES) {
        throw module_too_large_exception(
          ss::format("too many tables: {}, max: {}", vector_size, MAX_TABLES));
    }

    for (uint32_t i = 0; i < vector_size; ++i) {
        _tables.push_back(parse_table(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

mem parse_memory(iobuf_const_parser* parser) {
    return {.type = parse_memtype(parser)};
}

ss::future<>
module_builder::parse_memories_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_MEMORIES) {
        throw module_too_large_exception(ss::format(
          "too many memories: {}, max: {}", vector_size, MAX_MEMORIES));
    }

    for (uint32_t i = 0; i < vector_size; ++i) {
        _memories.push_back(parse_memory(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

value parse_const_expr(iobuf_const_parser* parser) {
    auto opcode = parser->consume_le_type<uint8_t>();
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    switch (opcode) {
    case 0x41:
        return value{.i32 = leb128::decode<uint32_t>(parser)};
    case 0x42:
        return value{.i64 = leb128::decode<uint64_t>(parser)};
    case 0x43:
        return value{.f32 = parser->consume_type<float>()};
    case 0x44:
        return value{.f64 = parser->consume_type<float>()};
    default:
        // TODO: Support refs, other global references, and vectors
        throw parse_exception(
          ss::format("unimplemented global value: {}", opcode));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

global parse_global(iobuf_const_parser* parser) {
    auto type = parse_globaltype(parser);
    auto value = parse_const_expr(parser);
    return {.type = type, .value = value};
}

ss::future<> module_builder::parse_globals_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_GLOBALS) {
        throw module_too_large_exception(ss::format(
          "too many globals: {}, max: {}", vector_size, MAX_GLOBALS));
    }

    for (uint32_t i = 0; i < vector_size; ++i) {
        _globals.push_back(parse_global(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

module_export module_builder::parse_one_export(iobuf_const_parser* parser) {
    auto name = parse_name(parser);
    auto type = parser->consume_le_type<uint8_t>();
    module_export::desc desc;
    switch (type) {
    case 0x00: { // func
        auto idx = parse_funcidx(parser);
        validate_in_range("unknown function export", idx, _functions);
        desc = idx;
        break;
    }
    case 0x01: { // table
        auto idx = parse_tableidx(parser);
        validate_in_range("unknown function export", idx, _tables);
        desc = idx;
        break;
    }
    case 0x02: { // memory
        auto idx = parse_memidx(parser);
        validate_in_range("unknown memory export", idx, _memories);
        desc = idx;
        break;
    }
    case 0x03: { // global
        auto idx = parse_globalidx(parser);
        validate_in_range("unknown global export", idx, _globals);
        desc = idx;
        break;
    }
    default:
        throw parse_exception(ss::format("unknown export type: {}", type));
    }
    return {.name = std::move(name), .description = desc};
}

ss::future<> module_builder::parse_exports_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > MAX_EXPORTS) {
        throw module_too_large_exception(ss::format(
          "too many exports: {}, max: {}", vector_size, MAX_EXPORTS));
    }

    for (uint32_t i = 0; i < vector_size; ++i) {
        _exports.push_back(parse_one_export(parser));
        co_await ss::coroutine::maybe_yield();
    }
}

std::vector<instruction>
parse_expression(iobuf_const_parser* parser, function_validator* validator) {
    std::vector<instruction> instruction_vector;
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    for (auto opcode = parser->consume_le_type<uint8_t>(); opcode != 0x0B;
         opcode = parser->consume_le_type<uint8_t>()) {
        instruction i;
        switch (opcode) {
        case 0x0F: // return
            i = op::return_values();
            break;
        case 0x20: { // get_local_i32
            auto idx = leb128::decode<uint32_t>(parser);
            i = op::get_local_i32(idx);
            break;
        }
        case 0x21: { // set_local_i32
            auto idx = leb128::decode<uint32_t>(parser);
            i = op::set_local_i32(idx);
            break;
        }
        case 0x41: { // const_i32
            auto v = leb128::decode<uint32_t>(parser);
            i = op::const_i32(value{.i32 = v});
            break;
        }
        case 0x6A: // add_i32
            i = op::add_i32();
            break;
        default:
            throw parse_exception(ss::format("unsupported opcode: {}", opcode));
        }
        // Validate the instruction is good.
        std::visit(*validator, i);
        instruction_vector.push_back(i);
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
    return instruction_vector;
}

void module_builder::parse_one_code(
  iobuf_const_parser* parser, function* func) {
    auto expected_size = leb128::decode<uint32_t>(parser);
    auto start_position = parser->bytes_consumed();

    auto vector_size = leb128::decode<uint32_t>(parser);
    for (uint32_t i = 0; i < vector_size; ++i) {
        auto num_locals = leb128::decode<uint32_t>(parser);
        if (num_locals + func->meta.locals.size() > MAX_FUNCTION_LOCALS) {
            throw module_too_large_exception(ss::format(
              "too many locals: {}", num_locals + func->meta.locals.size()));
        }
        auto valtype = parse_valtype(parser);
        std::fill_n(std::back_inserter(func->meta.locals), num_locals, valtype);
    }
    function_validator validator(func->meta.signature, func->meta.locals);
    func->body = parse_expression(parser, &validator);
    func->meta.max_stack_size_bytes = validator.maximum_stack_size_bytes();

    auto actual = parser->bytes_consumed() - start_position;
    if (actual != expected_size) {
        throw parse_exception(ss::format(
          "unexpected size of function, actual: {} expected: {}",
          actual,
          expected_size));
    }
}

ss::future<> module_builder::parse_code_section(iobuf_const_parser* parser) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    // We don't need to check the max size because we did that for _functions
    if (vector_size != _functions.size()) {
        throw parse_exception(ss::format(
          "unexpected number of code, actual: {} expected: {}",
          vector_size,
          _functions.size()));
    }
    // Check the number vs the function section
    for (uint32_t i = 0; i < vector_size; ++i) {
        auto& fn = _functions[i];
        parse_one_code(parser, &fn);
        co_await ss::coroutine::maybe_yield();
    }
}

ss::future<> module_builder::parse_one_section(iobuf_const_parser* parser) {
    auto id = parser->consume_le_type<uint8_t>();

    if (id != 0 && id <= _latest_section_read) {
        throw parse_exception(ss::format(
          "invalid section order, section id {} is after id {}",
          id,
          _latest_section_read));
    } else if (id != 0) {
        // Custom sections are allowed anywhere, and other sections need to
        // ensure are read in order.
        _latest_section_read = id;
    }
    auto size = leb128::decode<uint32_t>(parser);
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    switch (id) {
    case 0x00: // Custom section
        // Skip over custom sections for now
        // TODO: Support debug symbols if included.
        parser->skip(size);
        co_return;
    case 0x01: // type section
        co_await parse_signature_section(parser);
        co_return;
    case 0x02: // import section
        co_await parse_import_section(parser);
        co_return;
    case 0x03: // function section
        co_await parse_function_declaration_section(parser);
        co_return;
    case 0x04: // table section
        co_await parse_table_section(parser);
        co_return;
    case 0x05: // memory section
        co_await parse_memories_section(parser);
        co_return;
    case 0x06: // global section
        co_await parse_globals_section(parser);
        co_return;
    case 0x07: // export section
        co_await parse_exports_section(parser);
        co_return;
    case 0x08: { // start section
        auto start_funcidx = parse_funcidx(parser);
        validate_in_range("start function", start_funcidx, _functions);
        _start = start_funcidx;
        co_return;
    }
    case 0x09: // element section
        // TODO: Implement me
        throw parse_exception("tables unimplemented");
    case 0x0A: // code section
        co_await parse_code_section(parser);
        co_return;
    case 0x0B: // data section
    case 0x0C: // data count section
        // TODO: Implement me
        throw parse_exception("memories unimplemented");
    default:
        throw parse_exception(ss::format("unknown section id: {}", id));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

constexpr std::array<const uint8_t, 4> wasm_magic_bytes = {
  0x00, 0x61, 0x73, 0x6D};

ss::future<> module_builder::parse(iobuf_const_parser* parser) {
    bytes magic = parser->read_bytes(4);
    if (magic != to_bytes_view(wasm_magic_bytes)) {
        throw parse_exception(ss::format(
          "magic bytes mismatch: {:#x} {:#x} {:#x} {:#x}",
          magic[0],
          magic[1],
          magic[2],
          magic[3]));
    }
    auto version = parser->consume_le_type<int32_t>();
    if (version != 1) {
        throw parse_exception("unsupported wasm version");
    }
    module_builder builder;
    while (parser->bytes_left() > 0) {
        co_await builder.parse_one_section(parser);
        co_await ss::coroutine::maybe_yield();
    }
}

} // namespace

ss::future<parsed_module> parse_module(iobuf buffer) {
    iobuf_const_parser parser(buffer);
    module_builder builder;
    co_await builder.parse(&parser);
    co_return co_await std::move(builder).build();
}

} // namespace pandawasm
