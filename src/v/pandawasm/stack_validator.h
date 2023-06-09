/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "pandawasm/ast.h"
#include "pandawasm/instruction.h"
#include "pandawasm/value.h"
#include "utils/fragmented_vector.h"

#include <exception>
namespace pandawasm {

// Thrown if the wasm function is ill-formed.
class validation_exception : public std::exception {};

// A representation of the possible types instructions can take consume and
// produce. Some instructions are polymorphic, hence the need for additional
// information on top of `valtype`.
//
// Spec ref:
// https://webassembly.github.io/spec/core/valid/instructions.html#polymorphism
class validation_type {
public:
    explicit validation_type(valtype);

    static validation_type any();
    static validation_type anyref();

    bool is_i32() const;
    bool is_any() const;
    bool is_anyref() const;
    bool is_ref() const;
    size_t size_bytes() const;

    friend bool operator==(validation_type, validation_type);
    friend std::ostream& operator<<(std::ostream&, validation_type);

private:
    validation_type() = default;

    static constexpr uint8_t kAnyTypeValue = 0;
    static constexpr uint8_t kAnyRefTypeValue = 1;

    // This is a valtype or a polymorphic type above.
    uint8_t _type{0};
};

/**
 * The stack validator verifies that the stack operated on by a function is
 * well-formed.
 *
 * This ensures there will never be a case (at runtime) where the stack
 * could be popped when it's empty.
 *
 * Additionally computes the maximum memory usage for this function in terms of
 * the runtime stack. This can be used by a runtime to determine if
 *
 * Spec ref: https://webassembly.github.io/spec/core/valid/index.html
 *
 */
class stack_validator {
public:
    explicit stack_validator(function_type);

    // The maximum number of elements that are ever on the stack at
    // once.
    size_t maximum_stack_elements() const;
    // The maximum bytes that is used by the stack for this function at runtime.
    size_t maximum_stack_memory() const;

    void operator()(const op::const_i32&);
    void operator()(const op::add_i32&);
    void operator()(const op::get_local_i32&);
    void operator()(const op::set_local_i32&);
    void operator()(const op::return_values&);

    void finalize();

private:
    // Assert the correct type is popped
    void pop(validation_type);
    void pop(valtype);
    // Push the value on the stack
    void push(validation_type);
    void push(valtype);
    // Assert the stack is empty
    void assert_empty() const;
    // Check if the stack is empty
    bool empty() const;

    function_type _ft;

    fragmented_vector<validation_type> _underlying;
    size_t _current_memory_usage{0};
    size_t _max_stack_size{0};
    size_t _max_memory_usage{0};
};
} // namespace pandawasm
