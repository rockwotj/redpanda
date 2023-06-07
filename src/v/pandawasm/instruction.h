/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "value.h"

#include <variant>

#pragma once

namespace pandawasm {

namespace op {
// Push the constant onto the top of the stack.
struct const_i32 {
    constexpr static std::array<valtype, 0> stack_params = {};
    constexpr static std::array stack_results = {valtype::i32};

    value v;
};
struct add_i32 {
    constexpr static std::array stack_params = {valtype::i32, valtype::i32};
    constexpr static std::array stack_results = {valtype::i32};
};
// Push the local indexed by `idx` onto the top of the stack.
struct get_local_i32 {
    constexpr static std::array<valtype, 0> stack_params = {};
    constexpr static std::array stack_results = {valtype::i32};
    int32_t idx;
};
// Pop the top of the stack into local indexed by `idx`.
struct set_local_i32 {
    constexpr static std::array stack_params = {valtype::i32};
    constexpr static std::array<valtype, 0> stack_results = {};
    int32_t idx;
};
// Return the rest of the stack to the caller.
struct return_values {};
} // namespace op

using instruction = std::variant<
  op::const_i32,
  op::add_i32,
  op::get_local_i32,
  op::set_local_i32,
  op::return_values>;

} // namespace pandawasm
