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

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace pandawasm {
bool is_32bit(valtype vt) {
    switch (vt) {
    case valtype::i32:
    case valtype::f32:
        return true;
    default:
        return false;
    }
}
bool is_64bit(valtype vt) {
    switch (vt) {
    case valtype::i64:
    case valtype::f64:
    case valtype::externref:
    case valtype::funcref:
        return true;
    default:
        return false;
    }
}
std::ostream& operator<<(std::ostream& os, valtype vt) {
    switch (vt) {
    case valtype::i32:
        return os << "i32";
    case valtype::i64:
        return os << "i64";
    case valtype::f32:
        return os << "f32";
    case valtype::f64:
        return os << "f64";
    case valtype::v128:
        return os << "v128";
    case valtype::funcref:
        return os << "funcref";
    case valtype::externref:
        return os << "externref";
    }
    return os << "unknown";
}
std::ostream& operator<<(std::ostream& os, value v) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-union-access)
    return os << ss::format("{:x}", v.i64);
}
} // namespace pandawasm
