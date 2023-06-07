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
#include <asmjit/x86/x86assembler.h>
#include <asmjit/x86/x86operand.h>

#include <exception>
#include <memory>

namespace pandawasm {

class compilation_exception : public std::exception {
public:
    explicit compilation_exception(std::string msg)
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    std::string _msg;
};
struct runtime_value_location;
class runtime_value_location_stack;
class jit_function_compiler {
public:
    jit_function_compiler(
      asmjit::CodeHolder*, function_type ft, std::vector<valtype> locals);
    jit_function_compiler(const jit_function_compiler&) = delete;
    jit_function_compiler& operator=(const jit_function_compiler&) = delete;
    jit_function_compiler(jit_function_compiler&&) = delete;
    jit_function_compiler& operator=(jit_function_compiler&&) = delete;
    ~jit_function_compiler() = default;

    void prologue();
    void epilogue();

    void operator()(op::const_i32);
    void operator()(op::add_i32);
    void operator()(op::get_local_i32);
    void operator()(op::set_local_i32);
    void operator()(op::return_values);

private:
    asmjit::x86::Gpq ensure_in_reg(runtime_value_location*);
    asmjit::x86::Gpq allocate_register(runtime_value_location*);

    asmjit::x86::Assembler _asm;
    function_type _ft;
    // Does not include params in this list.
    std::vector<valtype> _locals;
    std::unique_ptr<runtime_value_location_stack> _stack;
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
    add_func(function_type, std::vector<valtype> locals);

private:
    asmjit::JitRuntime _rt;
    asmjit::CodeHolder _code;
};

} // namespace pandawasm
