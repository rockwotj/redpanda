#include "validator.h"

#include "pandawasm/value.h"
#include "vassert.h"

#include <algorithm>
#include <utility>
#include <vector>

namespace pandawasm {

validation_type::validation_type(valtype vt)
  : _type(uint8_t(vt)) {}

validation_type validation_type::any() {
    validation_type vt;
    vt._type = 0;
    return vt;
}
validation_type validation_type::anyref() {
    validation_type vt;
    vt._type = 1;
    return vt;
}
bool validation_type::is_i32() const { return _type == uint8_t(valtype::i32); }
bool validation_type::is_any() const { return _type == kAnyTypeValue; }
bool validation_type::is_anyref() const { return _type == kAnyRefTypeValue; }
bool validation_type::is_ref() const {
    return _type == kAnyRefTypeValue || _type == uint8_t(valtype::funcref)
           || _type == uint8_t(valtype::externref);
}
size_t validation_type::size_bytes() const {
    switch (_type) {
    case uint8_t(valtype::i32):
        return sizeof(int32_t);
    case uint8_t(valtype::i64):
        return sizeof(uint64_t);
    case uint8_t(valtype::f32):
        return sizeof(float);
    case uint8_t(valtype::f64):
        return sizeof(double);
    case kAnyTypeValue: // Assume worst case for anytype
    case uint8_t(valtype::v128):
        return sizeof(uint64_t) * 2;
    case kAnyRefTypeValue:
    case uint8_t(valtype::funcref):
    case uint8_t(valtype::externref):
        return sizeof(void*);
    default:
        vassert(false, "unknown validation type: {}", _type);
    }
}
bool operator==(validation_type lhs, validation_type rhs) {
    return lhs._type == rhs._type;
}
std::ostream& operator<<(std::ostream& os, validation_type vt) {
    switch (vt._type) {
    case uint8_t(valtype::i32):
        return os << "i32";
    case uint8_t(valtype::i64):
        return os << "i64";
    case uint8_t(valtype::f32):
        return os << "f32";
    case uint8_t(valtype::f64):
        return os << "f64";
    case validation_type::kAnyTypeValue:
        return os << "{any}";
    case uint8_t(valtype::v128):
        return os << "v128";
    case validation_type::kAnyRefTypeValue:
        return os << "{anyref}";
    case uint8_t(valtype::funcref):
        return os << "funcref";
    case uint8_t(valtype::externref):
        return os << "externref";
    default:
        vassert(false, "unknown validation type: {}", vt._type);
    }
}
function_validator::function_validator(
  function_signature ft, const std::vector<valtype>& locals)
  : _locals(std::move(ft.parameter_types))
  , _returns(std::move(ft.result_types)) {
    std::copy(locals.begin(), locals.end(), std::back_inserter(_locals));
}

size_t function_validator::maximum_stack_elements() const {
    return _max_stack_size;
}
size_t function_validator::maximum_stack_size_bytes() const {
    return _max_memory_usage;
}

void function_validator::operator()(const op::const_i32&) {
    push(valtype::i32);
}
void function_validator::operator()(const op::add_i32&) {
    pop(valtype::i32);
    pop(valtype::i32);
    push(valtype::i32);
}
void function_validator::operator()(const op::get_local_i32& op) {
    assert_local(op.idx, valtype::i32);
    push(valtype::i32);
}
void function_validator::operator()(const op::set_local_i32& op) {
    pop(valtype::i32);
    assert_local(op.idx, valtype::i32);
}
void function_validator::operator()(const op::return_values&) {
    for (valtype vt : _returns) {
        pop(vt);
    }
    assert_empty();
    // Mark the current block as unreachable.
    _unreachable = true;
}

void function_validator::finalize() {
    for (valtype vt : _returns) {
        pop(vt);
    }
    assert_empty();
}
bool function_validator::empty() const { return _underlying.empty(); }

void function_validator::assert_local(size_t idx, valtype vt) const {
    if (idx >= _locals.size() || _locals[idx] != vt) [[unlikely]] {
        throw validation_exception();
    }
}
void function_validator::assert_empty() const {
    if (!_underlying.empty()) [[unlikely]] {
        throw validation_exception();
    }
}
void function_validator::pop(valtype vt) { pop(validation_type(vt)); }
void function_validator::pop(validation_type expected) {
    validation_type actual = validation_type::any();
    if (_underlying.empty()) {
        if (!_unreachable) {
            throw validation_exception();
        }
    } else {
        actual = _underlying.back();
        _underlying.pop_back();
    }
    _current_memory_usage -= actual.size_bytes();
    bool ok = actual == expected;
    if (actual.is_any() || expected.is_any()) {
        ok = true;
    } else if (expected.is_anyref()) {
        ok = actual.is_ref();
    }
    if (!ok) {
        throw validation_exception();
    }
}
void function_validator::push(valtype vt) { push(validation_type(vt)); }

void function_validator::push(validation_type vt) {
    _underlying.push_back(vt);
    _current_memory_usage += vt.size_bytes();
    _max_stack_size = std::max(_max_stack_size, _underlying.size());
    _max_memory_usage = std::max(_max_memory_usage, _current_memory_usage);
}
} // namespace pandawasm
