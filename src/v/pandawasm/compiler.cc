#include "compiler.h"

#include "pandawasm/ast.h"
#include "units.h"

#include <asmjit/core/compiler.h>
#include <asmjit/core/compilerdefs.h>
#include <asmjit/core/constpool.h>
#include <asmjit/core/func.h>
#include <asmjit/core/type.h>
#include <asmjit/x86/x86compiler.h>
#include <asmjit/x86/x86operand.h>

#include <cstdint>
#include <stdexcept>

namespace pandawasm {

namespace {
asmjit::TypeId valtype_id(valtype vt) {
    switch (vt) {
    case valtype::i32:
        return asmjit::TypeId::kUInt32;
    case valtype::i64:
        return asmjit::TypeId::kUInt64;
    case valtype::f32:
        return asmjit::TypeId::kFloat32;
    case valtype::f64:
        return asmjit::TypeId::kFloat64;
    case valtype::v128:
        return asmjit::TypeId::kUInt8x16;
    case valtype::funcref:
    case valtype::externref:
        return asmjit::TypeId::kUIntPtr;
    }
    throw std::runtime_error("Unknown valtype");
}
} // namespace

jit_compiler::jit_compiler()
  : _cc(&_code) {
    _code.init(_rt.environment(), _rt.cpuFeatures());
}

jit_compiler::~jit_compiler() = default;

std::unique_ptr<jit_function_compiler> jit_compiler::add_func(
  const function_type& ft, const std::vector<valtype>& locals) {
    asmjit::FuncSignatureBuilder b;
    for (valtype vt : ft.parameter_types) {
        b.addArg(valtype_id(vt));
    }
    if (ft.result_types.size() == 1) {
        b.setRet(valtype_id(ft.result_types.front()));
    } else if (ft.result_types.size() > 1) {
        // TODO: implement this!
        b.setRetT<asmjit::Type::UIntPtr>();
    } else {
        b.setRet(asmjit::TypeId::kVoid);
    }
    auto func = _cc.addFunc(b);
    std::vector<asmjit::x86::Gp> registers;
    for (size_t i = 0; i < ft.parameter_types.size(); ++i) {
        valtype vt = ft.parameter_types[i];
        auto param_reg = _cc.newGp(valtype_id(vt), "parameter", i);
        func->setArg(i, param_reg);
        registers.push_back(std::move(param_reg));
    }
    for (size_t i = 0; i < locals.size(); ++i) {
        valtype vt = locals[i];
        auto local_reg = _cc.newGp(valtype_id(vt), "local", i);
        registers.push_back(std::move(local_reg));
    }
    return std::make_unique<jit_function_compiler>(
      func, &_cc, std::move(registers));
}

jit_function_compiler::jit_function_compiler(
  asmjit::FuncNode* node,
  asmjit::x86::Compiler* cc,
  std::vector<asmjit::x86::Gp> registers)
  : _func_node(node)
  , _cc(cc)
  , _mem(_cc->newStack(1_KiB, sizeof(void*), "value stack"))
  , _registers(std::move(registers)) {}

void jit_function_compiler::operator()(op::const_i32 op) {
    asmjit::x86::Mem c = _cc->newInt32Const(
      asmjit::ConstPoolScope::kLocal, std::bit_cast<int32_t>(op.v.i32));
    auto a = _cc->newGp(asmjit::TypeId::kUInt32);
    _cc->mov(a, c);
    _cc->mov(_mem, a);
}
void jit_function_compiler::operator()(op::add_i32) {
    auto a = _cc->newGp(asmjit::TypeId::kUInt32);
    auto b = _cc->newGp(asmjit::TypeId::kUInt32);

    _cc->sub(_mem, sizeof(uint32_t));
    _cc->mov(a, _mem);
    _cc->sub(_mem, sizeof(uint32_t));
    _cc->mov(b, _mem);
    _cc->add(a, b);
    _cc->mov(_mem, a);
    _cc->add(_mem, sizeof(uint32_t));
}
void jit_function_compiler::operator()(op::get_local_i32 op) {
    _cc->mov(_mem, _registers[op.idx].r32());
    _cc->add(_mem, sizeof(uint32_t));
}
void jit_function_compiler::operator()(op::set_local_i32 op) {
    _cc->mov(_mem, _registers[op.idx].r32());
    _cc->sub(_mem, sizeof(uint32_t));
}
void jit_function_compiler::operator()(op::return_values) {
    if (_func_node->hasRet()) {
        // TODO: Determine the actual return value.
        auto a = _cc->newGp(asmjit::TypeId::kUInt32);
        _cc->mov(a, _mem);
        _cc->ret(a);
    } else {
        _cc->ret();
    }
}

} // namespace pandawasm
