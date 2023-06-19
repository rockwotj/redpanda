#include "compiler.h"

#include "bytes/oncore.h"
#include "pandawasm/ast.h"
#include "pandawasm/value.h"
#include "units.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/print.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <asmjit/core/codeholder.h>
#include <asmjit/core/compiler.h>
#include <asmjit/core/compilerdefs.h>
#include <asmjit/core/constpool.h>
#include <asmjit/core/emitter.h>
#include <asmjit/core/errorhandler.h>
#include <asmjit/core/func.h>
#include <asmjit/core/globals.h>
#include <asmjit/core/jitruntime.h>
#include <asmjit/core/logger.h>
#include <asmjit/core/type.h>
#include <asmjit/x86/x86compiler.h>
#include <asmjit/x86/x86operand.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>

// https://www.felixcloutier.com/x86/index.html
using namespace asmjit;

namespace pandawasm {

namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger compiler_log("pandawasm_compiler");

class seastar_log_adapter final : public Logger {
    Error _log(const char* data, size_t size) noexcept final {
        compiler_log.info("{}", std::string_view(data, size));
        return ErrorCode::kErrorOk;
    }
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static seastar_log_adapter logger_adapter;

void check(Error error) {
    if (error) [[unlikely]] {
        throw compilation_exception(DebugUtils::errorAsString(error));
    }
}

class throwing_error_handler final : public ErrorHandler {
    void handleError(Error, const char* message, BaseEmitter*) final {
        throw compilation_exception(message);
    }
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static throwing_error_handler error_handler{};

TypeId valtype_to_id(valtype vt) {
    switch (vt) {
    case valtype::i32:
        return TypeId::kInt32;
    case valtype::i64:
        return TypeId::kInt64;
    case valtype::f32:
        return TypeId::kFloat32;
    case valtype::f64:
        return TypeId::kFloat64;
    case valtype::v128:
        return TypeId::kInt8x16;
    case valtype::funcref:
    case valtype::externref:
        return TypeId::kUIntPtr;
    default:
        vassert(false, "Unknown valtype: {}", vt);
    }
}
size_t valtype_size(valtype vt) { return TypeUtils::sizeOf(valtype_to_id(vt)); }

x86::Gp downcast_reg(const x86::Gp& reg, valtype vt) {
    x86::Gp int_reg = reg.r32();
    x86::Gp long_reg = reg.r64();
    return is_32bit(vt) ? int_reg : long_reg;
}

} // namespace

struct runtime_value_location {
    valtype type;
    // The register holding the value.
    std::optional<x86::Gpq> reg;
    // Where this value lives in stack memory.
    size_t stack_pointer{0};
};

class register_tracker {
public:
    register_tracker() = default;
    register_tracker(const register_tracker&) = delete;
    register_tracker& operator=(const register_tracker&) = delete;
    register_tracker(register_tracker&&) = delete;
    register_tracker& operator=(register_tracker&&) = delete;
    ~register_tracker() = default;

    // "Scratch" registers any function is allowed to overwrite, and use for
    // anything you want without asking anybody.  "Preserved" registers have to
    // be put back ("save" the register) if you use them, currently we don't use
    // any
    //
    // A 64 bit linux machine passes function parameters in rdi, rsi, rdx, rcx,
    // r8, and r9.  Any additional parameters get pushed on the stack.
    static constexpr std::array scratch_registers = {
      // Function argument #1 in 64-bit Linux.
      x86::rdi,
      // Function argument #2 in 64-bit Linux.
      x86::rsi,
      // Function argument #3 in 64-bit Linux.
      x86::rdx,
      // Function argument #4 in 64-bit Linux.
      x86::rcx,
      // Function argument #5 in 64-bit Linux.
      x86::r8,
      // Function argument #6 in 64-bit Linux.
      x86::r9,
      x86::r10,
      x86::r11,
      // Values are returned from functions in this register.
      x86::rax,
    };
    static constexpr size_t num_gp_registers = 16;
    using mask_t = std::bitset<num_gp_registers>;

    std::optional<x86::Gpq> take_unused_register() {
        for (const auto& reg : scratch_registers) {
            if (!_used_registers_mask.test(reg.id())) {
                _used_registers_mask.set(reg.id());
                return reg;
            }
        }
        return std::nullopt;
    }

    void mark_unused(const x86::Gpq& reg) {
        _used_registers_mask.reset(reg.id());
    }

private:
    mask_t _used_registers_mask;
};

/**
 *
 *
 */
class runtime_value_location_stack {
public:
    explicit runtime_value_location_stack(
      size_t max_stack_size_bytes, register_tracker* reg_tracker)
      : _stack_pointer(max_stack_size_bytes)
      , _reg_tracker(reg_tracker) {
        (void)_reg_tracker;
    }

    runtime_value_location_stack(const runtime_value_location_stack&) = delete;
    runtime_value_location_stack& operator=(const runtime_value_location_stack&)
      = delete;
    runtime_value_location_stack(runtime_value_location_stack&&) = delete;
    runtime_value_location_stack& operator=(runtime_value_location_stack&&)
      = delete;
    ~runtime_value_location_stack() = default;

private:
    using underlying_t = ss::circular_buffer<runtime_value_location>;

public:
    runtime_value_location* push(runtime_value_location v) {
        _stack_pointer -= valtype_size(v.type);
        v.stack_pointer = _stack_pointer;
        _stack.push_back(std::move(v));
        return &_stack.back();
    }

    runtime_value_location pop() {
        vassert(!_stack.empty(), "Trying to pop an empty stack");
        auto top = std::move(_stack.back());
        _stack.pop_back();
        _stack_pointer += valtype_size(top.type);
        return top;
    }
    runtime_value_location* peek() { return &_stack.back(); }
    auto begin() { return std::reverse_iterator(_stack.begin()); }
    auto end() { return std::reverse_iterator(_stack.end()); }
    auto rbegin() { return _stack.begin(); }
    auto rend() { return _stack.end(); }

    size_t pointer() const { return _stack_pointer; }

private:
    size_t _stack_pointer;
    underlying_t _stack;
    register_tracker* _reg_tracker;
};

class jit_function_compiler {
public:
    jit_function_compiler(
      asmjit::JitRuntime*,
      std::unique_ptr<asmjit::CodeHolder>,
      function::metadata);
    jit_function_compiler(const jit_function_compiler&) = delete;
    jit_function_compiler& operator=(const jit_function_compiler&) = delete;
    jit_function_compiler(jit_function_compiler&&) = delete;
    jit_function_compiler& operator=(jit_function_compiler&&) = delete;
    ~jit_function_compiler();

    void prologue();
    void epilogue();
    void* finalize();

    void operator()(op::const_i32);
    void operator()(op::add_i32);
    void operator()(op::get_local_i32);
    void operator()(op::set_local_i32);
    void operator()(op::return_values);

    // Annotate the next instruction emitted
    //
    // The input string must outlive the next instruction emit, and probably
    // should only be static strings.
    void annotate_next(const char* msg) { _asm.setInlineComment(msg); }

    // Annotate the next instruction emitted
    //
    // The returned string must be alive when the next instruction is emitted,
    // asmjit doesn't free these strings.
    template<typename... A>
    [[nodiscard(
      "the comment must outlive emitting the next instruction")]] ss::sstring
    annotate_next(const char* fmt, A&&... args) {
        auto msg = ss::format(fmt, std::forward<A>(args)...);
        _asm.setInlineComment(msg.c_str());
        return msg;
    }

private:
    asmjit::x86::Gpq ensure_in_reg(runtime_value_location*);
    asmjit::x86::Gpq allocate_register();

    asmjit::JitRuntime* _rt;
    std::unique_ptr<asmjit::CodeHolder> _code;
    asmjit::x86::Assembler _asm;
    asmjit::FuncFrame _frame;
    asmjit::Label _exit_label;

    size_t _locals_size_bytes{0};
    // What is the offset of a local value in the stack?
    std::vector<size_t> _locals_stack_offset;
    function::metadata _func_meta;
    register_tracker _reg_tracker;
    runtime_value_location_stack _stack;
};

jit_compiler::jit_compiler() = default;
jit_compiler::~jit_compiler() = default;

ss::future<compiled_function> jit_compiler::compile(function fn) {
    auto ch = std::make_unique<CodeHolder>();
    ch->init(_rt.environment(), _rt.cpuFeatures());
    jit_function_compiler c(&_rt, std::move(ch), fn.meta);
    c.prologue();
    for (const auto& op : fn.body) {
        std::visit(c, op);
        co_await ss::coroutine::maybe_yield();
    }
    c.epilogue();
    co_return compiled_function(c.finalize());
}
void jit_compiler::release(compiled_function func) { _rt.release(func.get()); }

// A useful cheatsheet:
// https://cs.brown.edu/courses/cs033/docs/guides/x64_cheatsheet.pdf
jit_function_compiler::jit_function_compiler(
  JitRuntime* rt, std::unique_ptr<CodeHolder> code, function::metadata meta)
  : _rt(rt)
  , _code(std::move(code))
  , _asm(_code.get())
  , _exit_label(_asm.newLabel())
  , _func_meta(std::move(meta))
  , _stack(_func_meta.max_stack_size_bytes, &_reg_tracker) {
    expression_in_debug_mode(_code->setLogger(&logger_adapter));
    expression_in_debug_mode(
      _asm.addDiagnosticOptions(DiagnosticOptions::kValidateAssembler));
    _asm.setErrorHandler(&error_handler);
    for (const auto& result_type : _func_meta.signature.parameter_types) {
        TypeId vt = valtype_to_id(result_type);
        // Add the max stack size because we put the locals at the top of the
        // stack, then the runtime stack.
        _locals_stack_offset.push_back(
          _func_meta.max_stack_size_bytes + _locals_size_bytes);
        _locals_size_bytes += int32_t(TypeUtils::sizeOf(vt));
    }
    for (const auto& result_type : _func_meta.locals) {
        TypeId vt = valtype_to_id(result_type);
        _locals_stack_offset.push_back(
          _func_meta.max_stack_size_bytes + _locals_size_bytes);
        _locals_size_bytes += int32_t(TypeUtils::sizeOf(vt));
    }
}

jit_function_compiler::~jit_function_compiler() = default;

x86::Gpq jit_function_compiler::allocate_register() {
    // Steal a register from the bottom value on the stack.
    std::optional<x86::Gpq> reg = _reg_tracker.take_unused_register();
    if (reg) {
        return *reg;
    }
    for (auto& v : std::ranges::reverse_view(_stack)) {
        if (!v.reg.has_value()) {
            continue;
        }
        std::swap(reg, v.reg);
        // Spill the register to the stack.
        auto r = downcast_reg(reg.value(), v.type);
        annotate_next("spill onto stack");
        // rsp[sp] = r
        _asm.mov(x86::Mem(x86::rsp, int32_t(v.stack_pointer)), r);
    }
    vassert(reg, "cannot steal register");
    return *reg;
}

x86::Gpq jit_function_compiler::ensure_in_reg(runtime_value_location* loc) {
    if (!loc->reg.has_value()) {
        loc->reg = allocate_register();
        // Load the value from the stack.
        auto reg = downcast_reg(loc->reg.value(), loc->type);
        annotate_next("load from stack");
        // reg = rsp[sp]
        _asm.mov(reg, x86::Mem(x86::rsp, int32_t(loc->stack_pointer)));
    }
    return loc->reg.value();
}

void jit_function_compiler::prologue() {
    // TODO: Figure out which parameters are already in registers.
    // TODO: Save some caller registers if it helps register pressure.
    FuncSignatureBuilder b;
    for (valtype vt : _func_meta.signature.parameter_types) {
        b.addArg(valtype_to_id(vt));
    }

    switch (_func_meta.signature.result_types.size()) {
    case 0:
        b.setRetT<void>();
        break;
    case 1:
        b.addArg(valtype_to_id(_func_meta.signature.result_types.front()));
        break;
    default:
        // In the case of multiple return types, we return a pointer to the pack
        // of values
        b.setRetT<void*>();
        break;
    }
    FuncDetail func;
    check(func.init(b, _asm.environment()));

    check(_frame.init(func));
    // TODO: There are probably cases where it makes sence to save some caller
    // registers to reduce register pressure.
    //
    // _frame.setAllDirty(RegGroup::kGp);
    _frame.finalize();
    _asm.emitProlog(_frame);
    annotate_next("set locals stack space");
    // TODO: Check that this won't overflow the stack
    // rsp -= <stack_size>
    _asm.sub(
      x86::regs::rsp, _func_meta.max_stack_size_bytes + _locals_size_bytes);
    // For each parameter, we're going to push it onto it's proper place on the
    // stack.
    // TODO: Optimize this, we don't always need to spill these onto the stack.
    for (size_t i = 0; i < _func_meta.signature.parameter_types.size(); ++i) {
        valtype vt = _func_meta.signature.parameter_types[i];
        vassert(
          i < 6,
          "too many parameters for function",
          _func_meta.signature.parameter_types.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        const x86::Gp& reg = register_tracker::scratch_registers[i];
        auto r = is_32bit(vt) ? static_cast<x86::Gp>(reg.r32()) : reg.r64();
        auto comment = annotate_next("save_local_to_stack({})", i);
        _asm.mov(x86::Mem(x86::regs::rsp, int32_t(_locals_stack_offset[i])), r);
    }
}

void jit_function_compiler::epilogue() {
    annotate_next("epilog start");
    _asm.bind(_exit_label);
    if (!_func_meta.signature.result_types.empty()) {
        auto vt = _func_meta.signature.result_types.front();
        auto result_reg = downcast_reg(x86::regs::rax, vt);
        auto loc = _stack.pop();
        ensure_in_reg(&loc);
        auto stack_top_reg = downcast_reg(loc.reg.value(), loc.type);
        if (stack_top_reg != result_reg) {
            _asm.mov(result_reg, stack_top_reg);
        }
    }
    // rsp += <stack_size>
    _asm.add(
      x86::regs::rsp, _func_meta.max_stack_size_bytes + _locals_size_bytes);
    // TODO: Figure out how to pass multiple values back on the stack.
    _asm.emitEpilog(_frame);
}

void* jit_function_compiler::finalize() {
    void* func = nullptr;
    _rt->add(&func, _code.get());
    return func;
}

void jit_function_compiler::operator()(op::const_i32 op) {
    auto* top = _stack.push({.type = valtype::i32});
    ensure_in_reg(top);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-union-access)
    uint32_t v = op.v.i32;
    auto comment = annotate_next("const_i32({})", v);
    // reg = i32
    _asm.mov(top->reg->r32(), v);
}
void jit_function_compiler::operator()(op::add_i32) {
    auto x2 = _stack.pop();
    auto x2r = ensure_in_reg(&x2);
    auto* x1 = _stack.peek();
    auto x1r = ensure_in_reg(x1);
    annotate_next("add_i32");
    // x1r += x2r
    _asm.add(x1r.r32(), x2r.r32());
    _reg_tracker.mark_unused(x2r);
}
void jit_function_compiler::operator()(op::get_local_i32 op) {
    _stack.push({.type = valtype::i32});
    auto* top = _stack.peek();
    top->reg = allocate_register();
    auto offset = int32_t(_locals_stack_offset[op.idx]);
    auto comment = annotate_next("get_local_i32({})", op.idx);
    _asm.mov(top->reg->r32(), x86::Mem(x86::rsp, offset));
}
void jit_function_compiler::operator()(op::set_local_i32 op) {
    auto v = _stack.pop();
    auto vr = ensure_in_reg(&v);
    auto offset = int32_t(_locals_stack_offset[op.idx]);
    auto comment = annotate_next("set_local_i32({})", op.idx);
    _asm.mov(x86::Mem(x86::rsp, offset), vr);
    _reg_tracker.mark_unused(vr);
}
void jit_function_compiler::operator()(op::return_values) {
    _asm.jmp(_exit_label);
}

} // namespace pandawasm
