#include "rp_module.h"
namespace wasm {

void redpanda_module::prep_call(
  const model::record_batch_header& header, model::record& record) {
    _call_ctx.emplace(transform_context{
      .key = iobuf_parser(record.release_key()),
      .value = iobuf_parser(record.release_value()),
      .offset = model::offset(header.base_offset() + record.offset_delta()),
      .timestamp = model::timestamp(
        header.first_timestamp() + record.timestamp_delta()),
      .headers = std::exchange(record.headers(), {}),
      .record_attributes = record.attributes(),
      .timestamp_delta = record.timestamp_delta(),
      .offset_delta = record.offset_delta(),
    });
}
void redpanda_module::post_call_unclean() { _call_ctx = std::nullopt; }

model::record_batch::uncompressed_records redpanda_module::post_call() {
    model::record_batch::uncompressed_records records;
    records.reserve(_call_ctx->output_records.size());
    // TODO: Encapsulate this in a builder
    for (auto& output_record : _call_ctx->output_records) {
        int32_t k_size = output_record.key
                           ? int32_t(output_record.key->size_bytes())
                           : -1;
        int32_t v_size = output_record.value
                           ? int32_t(output_record.value->size_bytes())
                           : -1;
        auto size = sizeof(model::record_attributes::type) // attributes
                    + vint::vint_size(_call_ctx->timestamp_delta)
                    + vint::vint_size(_call_ctx->offset_delta)
                    + vint::vint_size(k_size) + std::max(k_size, 0)
                    + vint::vint_size(v_size) + std::max(v_size, 0)
                    + vint::vint_size(output_record.headers.size());
        for (auto& h : output_record.headers) {
            size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                    + vint::vint_size(h.value_size()) + h.value().size_bytes();
        }
        auto r = model::record(
          size,
          _call_ctx->record_attributes,
          _call_ctx->timestamp_delta,
          _call_ctx->offset_delta,
          k_size,
          std::move(output_record.key).value_or(iobuf{}),
          v_size,
          std::move(output_record.value).value_or(iobuf{}),
          std::move(output_record.headers));
        records.push_back(std::move(r));
    }
    _call_ctx = std::nullopt;
    return records;
}

read_result redpanda_module::read_key(
  input_record_handle handle, ffi::array<uint8_t> data) {
    if (handle != fixed_input_record_handle || !_call_ctx || !data) {
        return -1;
    }
    size_t remaining = _call_ctx->key.bytes_left();
    size_t amount = std::min(size_t(data.size()), remaining);
    _call_ctx->key.consume_to(amount, data.raw());
    return int32_t(amount);
}
read_result redpanda_module::read_value(
  input_record_handle handle, ffi::array<uint8_t> data) {
    if (handle != fixed_input_record_handle || !_call_ctx || !data) {
        return -1;
    }
    size_t remaining = _call_ctx->value.bytes_left();
    size_t amount = std::min(size_t(data.size()), remaining);
    _call_ctx->value.consume_to(amount, data.raw());
    return int32_t(amount);
}

output_record_handle redpanda_module::create_output_record() {
    if (!_call_ctx) {
        return std::numeric_limits<output_record_handle>::max();
    }
    auto idx = _call_ctx->output_records.size();
    if (idx > max_output_records) {
        return std::numeric_limits<output_record_handle>::max();
    }
    _call_ctx->output_records.emplace_back();
    return int32_t(idx);
}

write_result redpanda_module::write_key(
  output_record_handle handle, ffi::array<uint8_t> data) {
    if (
      !_call_ctx || !data || handle < 0
      || handle >= int32_t(_call_ctx->output_records.size())) {
        return -1;
    }
    auto& k = _call_ctx->output_records[handle].key;
    if (!k) {
        k.emplace();
    }
    // TODO: Define a limit here?
    k->append(data.raw(), data.size());
    return int32_t(data.size());
}

write_result redpanda_module::write_value(
  output_record_handle handle, ffi::array<uint8_t> data) {
    if (
      !_call_ctx || !data || handle < 0
      || handle >= int32_t(_call_ctx->output_records.size())) {
        return -1;
    }
    // TODO: Define a limit here?
    auto& v = _call_ctx->output_records[handle].value;
    if (!v) {
        v.emplace();
    }
    // TODO: Define a limit here?
    v->append(data.raw(), data.size());
    return int32_t(data.size());
}

int32_t redpanda_module::num_headers(input_record_handle handle) {
    if (!_call_ctx || handle != fixed_input_record_handle) {
        return -1;
    }
    return int32_t(_call_ctx->headers.size());
}

int32_t redpanda_module::find_header_by_key(
  input_record_handle handle, ffi::array<uint8_t> key) {
    if (!_call_ctx || !key || handle != fixed_input_record_handle) {
        return -1;
    }
    std::string_view needle(reinterpret_cast<char*>(key.raw()), key.size());
    for (int32_t i = 0; i < int32_t(_call_ctx->headers.size()); ++i) {
        if (_call_ctx->headers[i].key() == needle) {
            return i;
        }
    }
    return -2;
}
int32_t redpanda_module::get_header_key_length(
  input_record_handle handle, int32_t index) {
    if (
      !_call_ctx || handle != fixed_input_record_handle || index < 0
      || index >= int32_t(_call_ctx->headers.size())) {
        return -1;
    }
    return int32_t(_call_ctx->headers[index].key_size());
}
int32_t redpanda_module::get_header_value_length(
  input_record_handle handle, int32_t index) {
    if (
      !_call_ctx || handle != fixed_input_record_handle || index < 0
      || index >= int32_t(_call_ctx->headers.size())) {
        return -1;
    }
    return int32_t(_call_ctx->headers[index].value_size());
}
int32_t redpanda_module::get_header_key(
  input_record_handle handle, int32_t index, ffi::array<uint8_t> key) {
    if (
      !_call_ctx || !key || handle != fixed_input_record_handle || index < 0
      || index >= int32_t(_call_ctx->headers.size())) {
        return -1;
    }
    const iobuf& k = _call_ctx->headers[index].key();
    if (key.size() < k.size_bytes()) {
        return -2;
    }
    iobuf::iterator_consumer(k.cbegin(), k.cend())
      .consume_to(k.size_bytes(), key.raw());
    return int32_t(k.size_bytes());
}
int32_t redpanda_module::get_header_value(
  input_record_handle handle, int32_t index, ffi::array<uint8_t> value) {
    if (
      !_call_ctx || !value || handle != fixed_input_record_handle || index < 0
      || index >= int32_t(_call_ctx->headers.size())) {
        return -1;
    }
    const iobuf& v = _call_ctx->headers[index].value();
    if (value.size() < v.size_bytes()) {
        return -2;
    }
    iobuf::iterator_consumer(v.cbegin(), v.cend())
      .consume_to(v.size_bytes(), value.raw());
    return int32_t(v.size_bytes());
}
int32_t redpanda_module::append_header(
  output_record_handle handle,
  ffi::array<uint8_t> key,
  ffi::array<uint8_t> value) {
    if (
      !_call_ctx || handle < 0
      || handle >= int32_t(_call_ctx->output_records.size())) {
        return -1;
    }
    iobuf k;
    k.append(key.raw(), key.size());
    iobuf v;
    v.append(value.raw(), value.size());
    _call_ctx->output_records[handle].headers.emplace_back(
      key.size(), std::move(k), value.size(), std::move(v));
    return int32_t(key.size() + value.size());
}
} // namespace wasm
