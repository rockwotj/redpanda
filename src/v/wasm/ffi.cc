/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ffi.h"

namespace wasm::ffi {

std::ostream& operator<<(std::ostream& o, val_type vt) {
    switch (vt) {
    case val_type::i32:
        o << "i32";
        break;
    case val_type::i64:
        o << "i64";
        break;
    case val_type::f32:
        o << "f32";
        break;
    case val_type::f64:
        o << "f64";
        break;
    }
    return o;
}

} // namespace wasm::ffi
