/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include <initializer_list>
#define BOOST_TEST_MODULE validator

#include "outcome.h"
#include "pandawasm/ast.h"
#include "pandawasm/instruction.h"
#include "pandawasm/validator.h"
#include "pandawasm/value.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <optional>
#include <type_traits>

using pandawasm::instruction;
using pandawasm::module_validator;
using pandawasm::valtype;
using namespace pandawasm::op;

template<typename>
struct dependent_false : std::false_type {};

template<typename T>
constexpr valtype as_wasmtype() {
    if constexpr (std::is_same_v<int, T>) {
        return valtype::i32;
    } else if constexpr (std::is_same_v<long, T>) {
        return valtype::i64;
    } else if constexpr (std::is_same_v<float, T>) {
        return valtype::f32;
    } else if constexpr (std::is_same_v<double, T>) {
        return valtype::f64;
    } else {
        static_assert(dependent_false<T>::value, "Unknown wasmtype");
    }
}

template<typename... Args>
concept EmptyPack = sizeof...(Args) == 0;

template<typename... Rest>
std::vector<valtype> as_wasmtypes()
requires EmptyPack<Rest...>
{
    return {};
}

template<typename T, typename... R>
std::vector<valtype> as_wasmtypes() {
    std::vector<valtype> v{as_wasmtype<T>()};
    for (auto vt : as_wasmtypes<R...>()) {
        v.push_back(vt);
    }
    return v;
}

template<typename R, typename... A>
void check_instructions(
  const std::initializer_list<pandawasm::instruction>& ops) {
    pandawasm::function_signature ft{.parameter_types = as_wasmtypes<A...>()};
    if constexpr (!std::is_void_v<R>) {
        auto vt = as_wasmtype<R>();
        ft.result_types.push_back(vt);
    }
    auto sv = module_validator(ft);
    for (const auto& op : ops) {
        std::visit(sv, op);
    }
    sv.finalize();
}

template<typename T>
void assert_valid(const std::initializer_list<pandawasm::instruction>& ops) {
    BOOST_CHECK_NO_THROW(check_instructions<T>(ops));
}
template<typename T>
void assert_invalid(const std::initializer_list<pandawasm::instruction>& ops) {
    BOOST_CHECK_THROW(
      check_instructions<T>(ops), pandawasm::validation_exception);
}

BOOST_AUTO_TEST_CASE(validate_noop_func) {
    assert_valid<void>({
      return_values(),
    });
}
BOOST_AUTO_TEST_CASE(validate_good_return_sequence) {
    assert_valid<int>({
      const_i32(),
      return_values(),
    });
}
BOOST_AUTO_TEST_CASE(validate_good_add_sequence) {
    assert_valid<int>({
      const_i32(),
      const_i32(),
      add_i32(),
      return_values(),
    });
}
BOOST_AUTO_TEST_CASE(validate_missing_return_values) {
    assert_invalid<int>({
      const_i32(),
    });
}
BOOST_AUTO_TEST_CASE(validate_missing_int_add_sequence) {
    assert_invalid<int>({
      const_i32(),
      add_i32(),
      return_values(),
    });
}
BOOST_AUTO_TEST_CASE(validate_extra_int_add_sequence) {
    assert_invalid<int>({
      const_i32(),
      const_i32(),
      const_i32(),
      add_i32(),
      return_values(),
    });
}
