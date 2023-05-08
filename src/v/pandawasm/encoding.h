#pragma once

#include "bytes/iobuf_parser.h"
#include "likely.h"

#include <cstdint>

namespace pandawasm::encoding {

template<typename int_type>
bytes encode_leb128(int_type value) {
    bytes output;

    static_assert(
      sizeof(int_type) == 4 || sizeof(int_type) == 8,
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned leading_bit_mask = 0x80U;
    constexpr unsigned sign_bit_mask = 0x40U;
    if constexpr (std::is_signed_v<int_type>) {
        bool more = true;
        while (more) {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= 7;
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
        do {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= 7;
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
int_type decode_leb128(iobuf_const_parser& parser) {
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned continuation_bit_mask = 0x80U;
    static_assert(
      sizeof(int_type) == 4 || sizeof(int_type) == 8,
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned size = sizeof(int_type) * 8;
    int_type result = 0;
    unsigned shift = 0;
    uint8_t byte = continuation_bit_mask;
    while ((shift < size) && (byte & continuation_bit_mask)) {
        byte = parser.consume_type<uint8_t>();
        result |= int_type(byte & lower_seven_bits_mask) << shift;
        shift += 7;
    }

    if (byte & continuation_bit_mask) [[unlikely]] {
        // Overflow!
        throw decode_exception();
    }

    if constexpr (std::is_signed_v<int_type>) {
        constexpr unsigned size = sizeof(int_type) * 8;
        constexpr unsigned sign_bit_mask = 0x40U;
        if ((shift < size) && (byte & sign_bit_mask)) {
            // sign extend
            result |= ~int_type(0) << shift;
        }
    }

    return result;
}

} // namespace pandawasm::encoding
