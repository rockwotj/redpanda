/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/abi.h"

WASM_EXPORT(_start) void _start() {}
uint32_t redpanda_abi_version() { return 0; }

constexpr uint32_t kBufferSize = 4096;


EventErrorCode copy_header(
  InputRecordHandle input_handle,
  OutputRecordHandle output_handle,
  int32_t index) {
    int32_t key_len = get_header_key_length(input_handle, index);
    auto* key_data = static_cast<uint8_t*>(__builtin_alloca(key_len));
    int32_t key_result = get_header_key(input_handle, index, key_data, key_len);
    if (key_result < 0) {
        return 1;
    }
    int32_t value_len = get_header_value_length(input_handle, index);
    auto* value_data = static_cast<uint8_t*>(__builtin_alloca(value_len));
    int32_t value_result = get_header_value(
      input_handle, index, value_data, value_len);
    if (value_result < 0) {
        return 1;
    }
    int32_t result = append_header(
      output_handle, key_data, key_result, value_data, value_result);
    return result < 0 ? 1 : 0;
}

EventErrorCode redpanda_on_record(InputRecordHandle input_handle) {
    OutputRecordHandle output_handle = create_output_record();
    uint8_t buf[kBufferSize];
    ReadResult result = 1;
    while (result > 0) {
        result = read_key(input_handle, &buf[0], kBufferSize);
        if (result <= 0) {
            break;
        }
        result = write_key(output_handle, &buf[0], result);
    }
    if (result < 0) {
        return 1;
    }
    result = 1;
    while (result > 0) {
        result = read_value(input_handle, &buf[0], kBufferSize);
        if (result <= 0) {
            break;
        }
        result = write_value(output_handle, &buf[0], result);
    }
    if (result < 0) {
        return 1;
    }
    int32_t header_count = num_headers(input_handle);
    for (int32_t i = 0; i < header_count; ++i) {
        EventErrorCode code = copy_header(input_handle, output_handle, i);
        if (code != 0) {
            return code;
        }
    }
    return 0;
}
