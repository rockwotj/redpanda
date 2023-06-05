// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include "pandawasm/ast.h"
#include "pandawasm/instruction.h"

#include <asmjit/asmjit.h>
#include <asmjit/core/compiler.h>
#include <asmjit/x86.h>
#include <asmjit/x86/x86operand.h>

namespace pandawasm {

class jit_function_compiler {
public:
    jit_function_compiler(
      asmjit::FuncNode*,
      asmjit::x86::Compiler*,
      std::vector<asmjit::x86::Gp> registers);
    jit_function_compiler(const jit_function_compiler&) = delete;
    jit_function_compiler& operator=(const jit_function_compiler&) = delete;
    jit_function_compiler(jit_function_compiler&&) = delete;
    jit_function_compiler& operator=(jit_function_compiler&&) = delete;
    ~jit_function_compiler() = default;

    const asmjit::FuncNode* node() const;

    void operator()(op::const_i32);
    void operator()(op::add_i32);
    void operator()(op::get_local_i32);
    void operator()(op::set_local_i32);
    void operator()(op::return_values);

private:
    asmjit::FuncNode* _func_node;
    asmjit::x86::Compiler* _cc;
    asmjit::x86::Mem _mem;
    std::vector<asmjit::x86::Gp> _registers;
};

class jit_compiler {
public:
    jit_compiler();
    jit_compiler(const jit_compiler&) = delete;
    jit_compiler& operator=(const jit_compiler&) = delete;
    jit_compiler(jit_compiler&&) = delete;
    jit_compiler& operator=(jit_compiler&&) = delete;
    ~jit_compiler();

    std::unique_ptr<jit_function_compiler>
    add_func(const function_type&, const std::vector<valtype>& locals);

private:
    asmjit::JitRuntime _rt;
    asmjit::CodeHolder _code;
    asmjit::x86::Compiler _cc;
};

} // namespace pandawasm
