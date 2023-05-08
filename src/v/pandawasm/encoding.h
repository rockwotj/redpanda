#pragma once

#include "bytes/iobuf_parser.h"

#include <cstdint>

namespace pandawasm::encoding {

template<typename int_type>
bytes encode_leb128(int_type value) {
    bytes output;

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

template<typename int_type>
int_type decode_leb128(iobuf_const_parser& parser) {
    int_type result = 0;
    unsigned shift = 0;
    uint8_t byte = 0;
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned leading_bit_mask = 0x80U;
    do {
        byte = parser.consume_type<uint8_t>();
        result |= int_type(byte & lower_seven_bits_mask) << shift;
        shift += 7;
    } while (byte & leading_bit_mask);

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
