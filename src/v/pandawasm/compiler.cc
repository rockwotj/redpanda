#include "compiler.h"

#include "bytes/oncore.h"
#include "pandawasm/ast.h"
#include "pandawasm/value.h"
#include "units.h"

#include <seastar/core/circular_buffer.hh>

#include <__ranges/reverse_view.h>
#include <asmjit/core/codeholder.h>
#include <asmjit/core/compiler.h>
#include <asmjit/core/compilerdefs.h>
#include <asmjit/core/constpool.h>
#include <asmjit/core/emitter.h>
#include <asmjit/core/func.h>
#include <asmjit/core/globals.h>
#include <asmjit/core/type.h>
#include <asmjit/x86/x86compiler.h>
#include <asmjit/x86/x86operand.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>

// https://www.felixcloutier.com/x86/index.html
using namespace asmjit;

namespace pandawasm {

namespace {

void check(Error err) {
    if (err) [[unlikely]] {
        throw compilation_exception(DebugUtils::errorAsString(err));
    }
}

} // namespace

struct runtime_value_location {
    valtype type;
    // The register holding the value.
    std::optional<x86::Gpq> reg;
    // Where this value lives in stack memory.
    int32_t stack_pointer{0};
};
class runtime_value_location_stack {
private:
    // "Scratch" registers any function is allowed to overwrite, and use for
    // anything you want without asking anybody.  "Preserved" registers have to
    // be put back ("save" the register) if you use them, currently we don't use
    // any
    //
    // A 64 bit linux machine passes function parameters in rdi, rsi, rdx, rcx,
    // r8, and r9.  Any additional parameters get pushed on the stack.
    static constexpr std::array scratch_registers = {
      // Values are returned from functions in this register.
      x86::rax,
      // Function argument #4 in 64-bit Linux.
      x86::rcx,
      // Function argument #3 in 64-bit Linux.
      x86::rdx,
      // Function argument #2 in 64-bit Linux.
      x86::rsi,
      // Function argument #1 in 64-bit Linux.
      x86::rdi,
      // Function argument #5 in 64-bit Linux.
      x86::r8,
      // Function argument #6 in 64-bit Linux.
      x86::r9,
      x86::r10,
      x86::r11,
    };

    using underlying_t = ss::circular_buffer<runtime_value_location>;
    using mask_t = std::bitset<scratch_registers.size()>;

public:
    void push(runtime_value_location v) { _stack.push_back(std::move(v)); }

    runtime_value_location pop() {
        auto top = std::move(_stack.back());
        _stack.pop_back();
        return top;
    }
    runtime_value_location& peek() { return _stack.back(); }
    auto begin() { return std::reverse_iterator(_stack.begin()); }
    auto end() { return std::reverse_iterator(_stack.end()); }
    auto rbegin() { return _stack.begin(); }
    auto rend() { return _stack.end(); }

    std::optional<x86::Gpq> find_unused_register() {
        for (const auto& reg : scratch_registers) {
            if (!_used_registers_mask.test(reg.id())) {
                _used_registers_mask.set(reg.id());
                return reg;
            }
        }
        return std::nullopt;
    }

private:
    int32_t _stack_pointer{0};
    // uint64_t _stack_pointer_ceil{0};
    mask_t _used_registers_mask;
    underlying_t _stack;
};

jit_compiler::jit_compiler() {
    _code.init(_rt.environment(), _rt.cpuFeatures());
}

jit_compiler::~jit_compiler() = default;

std::unique_ptr<jit_function_compiler>
jit_compiler::add_func(function_type ft, std::vector<valtype> locals) {
    return std::make_unique<jit_function_compiler>(
      &_code, std::move(ft), std::move(locals));
}

// A useful cheatsheet:
// https://cs.brown.edu/courses/cs033/docs/guides/x64_cheatsheet.pdf
jit_function_compiler::jit_function_compiler(
  CodeHolder* ch, function_type ft, std::vector<valtype> locals)
  : _asm(ch)
  , _ft(std::move(ft))
  , _locals(std::move(locals))
  , _stack(std::make_unique<runtime_value_location_stack>()) {
    expression_in_debug_mode(
      _asm.addDiagnosticOptions(DiagnosticOptions::kValidateAssembler));
}

x86::Gpq jit_function_compiler::allocate_register(runtime_value_location* loc) {
    // Steal a register from the bottom value on the stack.
    std::optional<x86::Gpq> reg = _stack->find_unused_register();
    if (reg) {
        return *reg;
    }
    for (auto& v : std::ranges::reverse_view(*_stack)) {
        if (!v.reg.has_value()) {
            continue;
        }
        std::swap(reg, v.reg);
        // Spill the register to the stack.
        auto r = is_32bit(loc->type) ? (x86::Gp)loc->reg->r32()
                                     : loc->reg->r64();
        check(_asm.mov(r, x86::Mem(x86::r14, loc->stack_pointer * CHAR_BIT)));
    }
    vassert(reg, "cannot steal register");
    return *reg;
}

x86::Gpq jit_function_compiler::ensure_in_reg(runtime_value_location* loc) {
    if (!loc->reg.has_value()) {
        loc->reg = allocate_register(loc);
        // Load the value from the stack.
        auto reg = is_32bit(loc->type) ? (x86::Gp)loc->reg->r32()
                                       : loc->reg->r64();
        check(_asm.mov(x86::Mem(x86::r14, loc->stack_pointer * CHAR_BIT), reg));
    }
    return loc->reg.value();
}

void jit_function_compiler::prologue() {
    // TODO: Figure out which parameters are already in registers.
    // TODO: Save some caller registers if it helps register pressure.
    // TODO: Compute the actual size size based on max stack size and the number
    // of effective locals.
    check(_asm.sub(x86::rsp, 0x18));
}
void jit_function_compiler::epilogue() {
    // TODO: Make sure we're passing stuff back correctly
    check(_asm.add(x86::rsp, 0x18));
    check(_asm.ret());
}

void jit_function_compiler::operator()(op::const_i32 op) {
    runtime_value_location loc{
      .type = valtype::i32,
      .stack_pointer = _stack->stack_pointer,
    };
    auto reg = allocate_register(&loc);
    _stack->push(std::move(loc));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-union-access)
    check(_asm.mov(reg.r32(), op.v.i32));
}
void jit_function_compiler::operator()(op::add_i32) {
    auto x2 = _stack->pop();
    auto x2r = ensure_in_reg(&x2);
    auto& x1 = _stack->peek();
    auto x1r = ensure_in_reg(&x1);
    // x2r += x1r
    check(_asm.add(x2r, x1r));
}
void jit_function_compiler::operator()(op::get_local_i32 op) {
    runtime_value_location loc{
      .type = valtype::i32,
      .stack_pointer = _stack->stack_pointer,
    };
    // This could in a register already..?
    auto reg = allocate_register(&loc);
    // reg = r14[idx * 8];
    check(_asm.mov(reg.r32(), x86::Mem(x86::r14, op.idx * CHAR_BIT)));
}
void jit_function_compiler::operator()(op::set_local_i32) {
    _stack->pop();
    // TODO!
}
void jit_function_compiler::operator()(op::return_values) {
    // The actual return instruction will be emitted in epilogue
}

} // namespace pandawasm
