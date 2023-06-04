#include "value.h"

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace pandawasm {
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
    return os << ss::format("{:x}", v.i64);
}
} // namespace pandawasm
