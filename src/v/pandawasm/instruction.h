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
#include "value.h"

#include <variant>

#pragma once

namespace pandawasm {

class value_stack final {
public:
    void push(value);
    value pop();

private:
    std::vector<value> _underlying;
};

class frame final {
public:
    void set(uint32_t idx, value);
    value get(uint32_t idx) const;

private:
    std::vector<value> _underlying;
};

namespace op {
// Push the constant onto the top of the stack.
struct const_i32 {
    value v;
};
struct add_i32 {};
// Push the local indexed by `idx` onto the top of the stack.
struct get_local_i32 {
    uint32_t idx;
};
// Pop the top of the stack into local indexed by `idx`.
struct set_local_i32 {
    uint32_t idx;
};
// Return the rest of the stack to the caller.
struct retrn {};
} // namespace op

using instruction = std::variant<
  op::const_i32,
  op::add_i32,
  op::get_local_i32,
  op::set_local_i32,
  op::retrn>;

} // namespace pandawasm
