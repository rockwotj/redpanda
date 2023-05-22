#include "ast.h"

#pragma once

namespace pandawasm {

class value_stack final {
public:
    void push(value);
    value pop();

private:
    std::vector<value> _underlying;
};

class environment final {
public:
    void set(uint32_t idx, value);
    value get(uint32_t idx) const;

private:
    std::vector<value> _underlying;
};

using operation
  = void (*)(environment*, value_stack* sp, const instruction* pc);

union instruction {
    operation op;
    value value;
};

instruction op(operation);
instruction val(value);

namespace instructions {

void noop(environment*, value_stack*, const instruction*);
void const_i32(environment*, value_stack*, const instruction*);
void add_i32(environment*, value_stack*, const instruction*);
void get_local_i32(environment*, value_stack*, const instruction*);
void set_local_i32(environment*, value_stack*, const instruction*);
void retrn(environment*, value_stack*, const instruction*);

} // namespace instructions
} // namespace pandawasm
