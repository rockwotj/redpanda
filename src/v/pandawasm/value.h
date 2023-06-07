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
#include <cstdint>
#include <ostream>

namespace pandawasm {

/**
 * See: https://webassembly.github.io/spec/core/syntax/types.html
 */
enum class valtype : uint8_t {
    i32 = 0x7F,
    i64 = 0x7E,
    f32 = 0x7D,
    f64 = 0x7C,
    v128 = 0x7B,
    funcref = 0x70,
    externref = 0x6F,
};
bool is_32bit(valtype);
bool is_64bit(valtype);

std::ostream& operator<<(std::ostream&, valtype);

/**
 * See: https://webassembly.github.io/spec/core/syntax/types.html
 */
union value {
    uint32_t i32;
    uint64_t i64;
    float f32;
    double f64;
};
std::ostream& operator<<(std::ostream&, valtype);
} // namespace pandawasm
