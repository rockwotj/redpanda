#include "redpanda/abi.h"

WASM_EXPORT(_start) void _start() {}
uint32_t redpanda_abi_version() { return 0; }

constexpr uint32_t kBufferSize = 8 * 1024;
constexpr uint32_t kMaxTokens = 128;
struct string {
    const char* data;
    uint32_t len;
};
#define STRING_CONST(str)                                                      \
    { .data = (str), .len = sizeof(str) - 1 }

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
    // Assumes that prompt is < 4k
    result = read_value(input_handle, &buf[0], kBufferSize);
    if (result <= 0) {
      return 1;
    }
    string url = STRING_CONST(
        "https://api.openai.com/v1/completions");
    uint8_t body[kBufferSize];

    string body_prelude = STRING_CONST(R"({"model":"text-davinci-003", "max_tokens": 512, "prompt":")");
    string body_suffix = STRING_CONST(R"("})");
    __builtin_memcpy(&body[0], body_prelude.data, body_prelude.len);
    __builtin_memcpy(&body[body_prelude.len], &buf[0], result);
    __builtin_memcpy(
        &body[body_prelude.len + result],
        &body_suffix.data[0],
        body_suffix.len);
    string headers = STRING_CONST("Content-Type: application/json\n"
        "Authorization: Bearer "
        OPENAI_API_KEY);

    HttpResponseHandle handle = http_fetch(
        http_method::POST,
        reinterpret_cast<const uint8_t*>(url.data),
        url.len,
        reinterpret_cast<const uint8_t*>(headers.data),
        headers.len,
        &body[0],
        body_prelude.len + result + body_suffix.len);

    if (handle < 0) {
      return 1;
    }

    result = read_http_resp_body(handle, &buf[0], kBufferSize);
    if (result < 0) {
      return 1;
    }
    result = write_value(output_handle, &buf[0], result);
    if (result < 0) {
        return 1;
    }
    return 0;
}
