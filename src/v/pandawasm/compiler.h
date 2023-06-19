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
#include <asmjit/core/codeholder.h>
#include <asmjit/core/compiler.h>
#include <asmjit/core/errorhandler.h>
#include <asmjit/core/jitruntime.h>
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

class compiled_function {
public:
    explicit compiled_function(void* ptr)
      : _ptr(ptr) {}

    template<typename R, typename... A>
    R invoke(A... args) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return reinterpret_cast<R (*)(A...)>(_ptr)(args...);
    }

    void* get() { return _ptr; }

private:
    void* _ptr;
};

class jit_compiler {
public:
    jit_compiler();
    jit_compiler(const jit_compiler&) = delete;
    jit_compiler& operator=(const jit_compiler&) = delete;
    jit_compiler(jit_compiler&&) = delete;
    jit_compiler& operator=(jit_compiler&&) = delete;
    ~jit_compiler();

    ss::future<compiled_function> compile(function);
    void release(compiled_function func);

private:
    asmjit::JitRuntime _rt;
};

} // namespace pandawasm
