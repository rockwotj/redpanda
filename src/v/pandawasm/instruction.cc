#include "instruction.h"

#define TAILCALL() [[clang::musttail]] return
#define NEXTOP() TAILCALL() pc->op(env, s, pc + 1)

namespace pandawasm {

void value_stack::push(value v) { _underlying.push_back(v); }

value value_stack::pop() {
    value v = _underlying.back();
    _underlying.pop_back();
    return v;
}

void environment::set(uint32_t idx, value v) { _underlying[idx] = v; }
value environment::get(uint32_t idx) const { return _underlying[idx]; }

instruction op(operation o) {
  return {.op = o};
}
instruction val(value v) {
  return {.value = v};
}

namespace instructions {

void noop(environment* env, value_stack* s, instruction* pc) { NEXTOP(); }
void const_i32(environment* env, value_stack* s, instruction* pc) {
    s->push((pc++)->value);
    NEXTOP();
}
void add_i32(environment* env, value_stack* s, instruction* pc) {
    value result = {.i32 = s->pop().i32 + s->pop().i32};
    s->push(result);
    NEXTOP();
}
void get_local_i32(environment* env, value_stack* s, instruction* pc) {
    s->push(env->get((pc++)->value.i32));
    NEXTOP();
}
void set_local_i32(environment* env, value_stack* s, instruction* pc) {
    env->set((pc++)->value.i32, s->pop());
    NEXTOP();
}
void retrn(environment*, value_stack*, instruction*) { return; }

}
