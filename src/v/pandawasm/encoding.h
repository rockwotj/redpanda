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

#include "bytes/iobuf_parser.h"
#include "likely.h"

#include <climits>
#include <cstdint>

namespace pandawasm::leb128 {

template<typename int_type>
bytes encode(int_type value) {
    bytes output;

    static_assert(
      sizeof(int_type) == sizeof(uint32_t)
        || sizeof(int_type) == sizeof(uint64_t),
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned leading_bit_mask = 0x80U;
    constexpr unsigned sign_bit_mask = 0x40U;
    if constexpr (std::is_signed_v<int_type>) {
        bool more = true;
        while (more) {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= CHAR_BIT - 1;
            if (
              ((value == 0) && !(byte & sign_bit_mask))
              || ((value == -1) && (byte & sign_bit_mask))) {
                more = false;
            } else {
                byte |= leading_bit_mask;
            }
            output.append(&byte, 1);
        }
    } else {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while)
        do {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= CHAR_BIT - 1;
            if (value != 0) {
                byte |= leading_bit_mask;
            }
            output.append(&byte, 1);
        } while (value != 0);
    }

    return output;
}

class decode_exception : std::exception {};

template<typename int_type>
int_type decode(iobuf_const_parser* parser) {
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned continuation_bit_mask = 0x80U;
    static_assert(
      sizeof(int_type) == sizeof(uint32_t)
        || sizeof(int_type) == sizeof(uint64_t),
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned size = sizeof(int_type) * CHAR_BIT;
    int_type result = 0;
    unsigned shift = 0;
    uint8_t byte = continuation_bit_mask;
    while ((shift < size) && (byte & continuation_bit_mask)) {
        byte = parser->consume_type<uint8_t>();
        result |= int_type(byte & lower_seven_bits_mask) << shift;
        shift += CHAR_BIT - 1;
    }

    if (byte & continuation_bit_mask) [[unlikely]] {
        // Overflow!
        throw decode_exception();
    }

    if constexpr (std::is_signed_v<int_type>) {
        constexpr unsigned size = sizeof(int_type) * CHAR_BIT;
        constexpr unsigned sign_bit_mask = 0x40U;
        if ((shift < size) && (byte & sign_bit_mask)) {
            // sign extend
            result |= ~int_type(0) << shift;
        }
    }

    return result;
}

} // namespace pandawasm::leb128
