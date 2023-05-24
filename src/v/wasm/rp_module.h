#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "wasm/ffi.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace wasm {
using read_result = int32_t;
using write_result = int32_t;
using input_record_handle = int32_t;
using output_record_handle = int32_t;

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;
constexpr size_t max_output_records = 256;
constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";

struct record_builder {
    std::optional<iobuf> key;
    std::optional<iobuf> value;
    std::vector<model::record_header> headers;
};

struct transform_context {
    iobuf_parser key;
    iobuf_parser value;
    model::offset offset;
    model::timestamp timestamp;

    std::vector<model::record_header> headers;

    model::record_attributes record_attributes;
    int64_t timestamp_delta;
    int32_t offset_delta;
    std::vector<record_builder> output_records;
};

class redpanda_module {
public:
    redpanda_module() = default;
    redpanda_module(const redpanda_module&) = delete;
    redpanda_module& operator=(const redpanda_module&) = delete;
    redpanda_module(redpanda_module&&) = default;
    redpanda_module& operator=(redpanda_module&&) = default;
    ~redpanda_module() = default;

    static constexpr std::string_view name = "redpanda";

    void
    prep_call(const model::record_batch_header& header, model::record& record);

    void post_call_unclean();

    model::record_batch::uncompressed_records post_call();

    // Start ABI exports
    // This is a small set just to get the ball rolling

    read_result read_key(input_record_handle handle, ffi::array<uint8_t> data);

    read_result
    read_value(input_record_handle handle, ffi::array<uint8_t> data);

    output_record_handle create_output_record();

    write_result
    write_key(output_record_handle handle, ffi::array<uint8_t> data);

    write_result
    write_value(output_record_handle handle, ffi::array<uint8_t> data);

    int32_t num_headers(input_record_handle handle);

    int32_t
    find_header_by_key(input_record_handle handle, ffi::array<uint8_t> key);

    int32_t get_header_key_length(input_record_handle handle, int32_t index);

    int32_t get_header_value_length(input_record_handle handle, int32_t index);

    int32_t get_header_key(
      input_record_handle handle, int32_t index, ffi::array<uint8_t> key);

    int32_t get_header_value(
      input_record_handle handle, int32_t index, ffi::array<uint8_t> value);

    int32_t append_header(
      output_record_handle handle,
      ffi::array<uint8_t> key,
      ffi::array<uint8_t> value);

    // End ABI exports

private:
    std::optional<transform_context> _call_ctx;
};
} // namespace wasm
