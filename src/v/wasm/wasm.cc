#include "wasm.h"

#include "bytes/bytes.h"
#include "errc.h"
#include "http/client.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/timestamp.h"
#include "net/tls.h"
#include "net/unresolved_address.h"
#include "outcome.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"
#include "utils/mutex.h"
#include "utils/uri.h"
#include "utils/vint.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/print.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/tls.hh>

#include <absl/base/casts.h>
#include <absl/strings/str_split.h>
#include <boost/beast/http/field.hpp>
#include <boost/fusion/sequence/io/out.hpp>
#include <boost/regex.hpp>
#include <boost/type_traits/function_traits.hpp>
#include <wasmedge/enum_types.h>
#include <wasmedge/wasmedge.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ratio>
#include <regex>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <variant>

namespace wasm {

namespace {

static ss::logger wasm_log("wasm");

// TODO: Use a struct so there is no need for the fn pointer storage
using WasmEdgeConfig = std::
  unique_ptr<WasmEdge_ConfigureContext, decltype(&WasmEdge_ConfigureDelete)>;
using WasmEdgeStore
  = std::unique_ptr<WasmEdge_StoreContext, decltype(&WasmEdge_StoreDelete)>;
using WasmEdgeVM
  = std::unique_ptr<WasmEdge_VMContext, decltype(&WasmEdge_VMDelete)>;
using WasmEdgeLoader
  = std::unique_ptr<WasmEdge_LoaderContext, decltype(&WasmEdge_LoaderDelete)>;
using WasmEdgeASTModule = std::
  unique_ptr<WasmEdge_ASTModuleContext, decltype(&WasmEdge_ASTModuleDelete)>;
using WasmEdgeModule = std::unique_ptr<
  WasmEdge_ModuleInstanceContext,
  decltype(&WasmEdge_ModuleInstanceDelete)>;
using WasmEdgeFuncType = std::unique_ptr<
  WasmEdge_FunctionTypeContext,
  decltype(&WasmEdge_FunctionTypeDelete)>;

class wasmedge_wasm_engine;

class wasm_exception : public std::exception {
public:
    explicit wasm_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

using read_result = int32_t;
using write_result = int32_t;
using input_record_handle = int32_t;
using output_record_handle = int32_t;

template<class T>
struct dependent_false : std::false_type {};

namespace ffi {
template<typename T>
class array {
public:
    using element_type = T;

    array()
      : _ptr(nullptr)
      , _size(0) {}
    array(T* ptr, uint32_t size)
      : _ptr(ptr)
      , _size(size) {}

    array(array<T>&&) noexcept = default;
    array& operator=(array<T>&&) noexcept = default;

    array(const array<T>&) noexcept = default;
    array& operator=(const array<T>&) noexcept = default;

    ~array() = default;

    explicit operator bool() const noexcept { return bool(_ptr); }

    T* raw() noexcept { return _ptr; }

    const T* raw() const noexcept { return _ptr; }

    T& operator[](uint32_t index) noexcept { return _ptr[index]; }

    const T& operator[](uint32_t index) const noexcept { return _ptr[index]; }

    uint32_t size() const noexcept { return _size; }

private:
    T* _ptr;
    uint32_t _size;
};

std::string_view array_as_string_view(array<uint8_t> arr) {
    return std::string_view(reinterpret_cast<char*>(arr.raw()), arr.size());
}

template<typename T>
struct is_array {
    static constexpr bool value = false;
};
template<template<typename...> class C, typename U>
struct is_array<C<U>> {
    static constexpr bool value = std::is_same<C<U>, array<U>>::value;
};

template<typename Type>
void transform_type(std::vector<WasmEdge_ValType>& types) {
    if constexpr (std::is_same_v<WasmEdge_MemoryInstanceContext*, Type>) {
        // Do nothing
    } else if constexpr (is_array<Type>::value) {
        // Push back an arg for the pointer
        types.push_back(WasmEdge_ValType_I32);
        // Push back an other arg for the length
        types.push_back(WasmEdge_ValType_I32);
    } else if constexpr (
      std::is_pointer_v<
        Type> || std::is_same_v<Type, uint16_t> || std::is_same_v<Type, int16_t> || std::is_same_v<Type, int32_t> || std::is_same_v<Type, uint32_t>) {
        types.push_back(WasmEdge_ValType_I32);
    } else if constexpr (
      std::is_same_v<Type, int64_t> || std::is_same_v<Type, uint64_t>) {
        types.push_back(WasmEdge_ValType_I64);
    } else if constexpr (std::is_same_v<Type, void>) {
        // There is nothing to do
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename... Args>
concept EmptyPack = sizeof...(Args) == 0;

template<typename... Rest>
void transform_types(
  std::vector<WasmEdge_ValType>&) requires EmptyPack<Rest...> {
    // Nothing to do
}

template<typename Type, typename... Rest>
void transform_types(std::vector<WasmEdge_ValType>& types) {
    transform_type<Type>(types);
    transform_types<Rest...>(types);
}

template<typename Type>
std::tuple<Type> extract_parameter(
  const WasmEdge_CallingFrameContext* calling_frame,
  const WasmEdge_Value* params,
  unsigned& idx) {
    if constexpr (std::is_same_v<WasmEdge_MemoryInstanceContext*, Type>) {
        auto* mem = WasmEdge_CallingFrameGetMemoryInstance(calling_frame, 0);
        return std::tuple(mem);
    } else if constexpr (is_array<Type>::value) {
        uint32_t guest_ptr = WasmEdge_ValueGetI32(params[idx++]);
        uint32_t ptr_len = WasmEdge_ValueGetI32(params[idx++]);
        auto* mem = WasmEdge_CallingFrameGetMemoryInstance(calling_frame, 0);
        if (mem == nullptr) {
            return std::tuple<Type>();
        }
        uint8_t* host_ptr = WasmEdge_MemoryInstanceGetPointer(
          mem, guest_ptr, ptr_len * sizeof(typename Type::element_type));
        if (host_ptr == nullptr) {
            return std::tuple<Type>();
        }
        return std::make_tuple(array<typename Type::element_type>(
          reinterpret_cast<typename Type::element_type*>(host_ptr), ptr_len));
    } else if constexpr (
      std::is_same_v<Type, const void*> || std::is_same_v<Type, void*>) {
        ++idx;
        // TODO: Remove this temporary hack
        return std::make_tuple(static_cast<Type>(nullptr));
    } else if constexpr (std::is_pointer_v<Type>) {
        // Assume this is an out val
        uint32_t guest_ptr = WasmEdge_ValueGetI32(params[idx++]);
        uint32_t ptr_len = sizeof(typename std::remove_pointer_t<Type>);
        auto* mem = WasmEdge_CallingFrameGetMemoryInstance(calling_frame, 0);
        if (mem == nullptr) {
            return std::tuple<Type>();
        }
        uint8_t* host_ptr = WasmEdge_MemoryInstanceGetPointer(
          mem, guest_ptr, ptr_len);
        if (host_ptr == nullptr) {
            return std::tuple<Type>();
        }
        return std::make_tuple(reinterpret_cast<Type>(host_ptr));
    } else if constexpr (
      std::is_same_v<Type, int16_t> || std::is_same_v<Type, uint16_t>) {
        return std::make_tuple(
          static_cast<Type>(WasmEdge_ValueGetI32(params[idx++])));
    } else if constexpr (
      std::is_same_v<Type, int32_t> || std::is_same_v<Type, uint32_t>) {
        return std::make_tuple(
          std::bit_cast<Type>(WasmEdge_ValueGetI32(params[idx++])));
    } else if constexpr (
      std::is_same_v<Type, int64_t> || std::is_same_v<Type, uint64_t>) {
        return std::make_tuple(
          std::bit_cast<Type>(WasmEdge_ValueGetI64(params[idx++])));
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename... Rest>
std::tuple<> extract_parameters(
  const WasmEdge_CallingFrameContext*,
  const WasmEdge_Value*,
  unsigned) requires EmptyPack<Rest...> {
    return std::make_tuple();
}

template<typename Type, typename... Rest>
std::tuple<Type, Rest...> extract_parameters(
  const WasmEdge_CallingFrameContext* calling_ctx,
  const WasmEdge_Value* params,
  unsigned idx) {
    auto head_type = extract_parameter<Type>(calling_ctx, params, idx);
    return std::tuple_cat(
      std::move(head_type),
      extract_parameters<Rest...>(calling_ctx, params, idx));
}

template<typename Type>
void pack_result(WasmEdge_Value* results, Type result) {
    if constexpr (
      std::is_same_v<Type, uint16_t> || std::is_same_v<Type, int16_t>) {
        *results = WasmEdge_ValueGenI32(static_cast<int32_t>(result));
    } else if constexpr (
      std::is_same_v<Type, int32_t> || std::is_same_v<Type, uint32_t>) {
        *results = WasmEdge_ValueGenI32(std::bit_cast<int32_t>(result));
    } else if constexpr (
      std::is_same_v<Type, int64_t> || std::is_same_v<Type, uint64_t>) {
        *results = WasmEdge_ValueGenI64(std::bit_cast<int64_t>(result));
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}
} // namespace ffi

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;
constexpr size_t max_output_records = 256;

constexpr std::string_view redpanda_module_name = "redpanda";
constexpr std::string_view wasi_preview_1_module_name
  = "wasi_snapshot_preview1";
constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";
constexpr std::string_view wasi_preview_1_start_function_name = "_start";

WasmEdgeModule create_module(std::string_view name) {
    auto wrapped = WasmEdge_StringWrap(name.data(), name.size());
    return {
      WasmEdge_ModuleInstanceCreate(wrapped), &WasmEdge_ModuleInstanceDelete};
}

struct record_builder {
    std::optional<iobuf> key;
    std::optional<iobuf> value;
    std::vector<model::record_header> headers;
};

struct transform_context {
    iobuf::iterator_consumer key;
    iobuf::iterator_consumer value;
    model::offset offset;
    model::timestamp timestamp;
    std::vector<model::record_header> headers;

    std::vector<iobuf_parser> http_responses;
    std::vector<record_builder> output_records;
};

class wasmedge_wasm_engine : public engine {
public:
    static result<std::unique_ptr<wasmedge_wasm_engine>, errc>
      create(std::string_view);

    ss::future<model::record_batch>
    transform(model::record_batch&& batch) override {
        model::record_batch decompressed
          = co_await storage::internal::decompress_batch(std::move(batch));

        std::vector<model::record> transformed_records;

        auto header = decompressed.header();
        auto new_header = model::record_batch_header{
          .size_bytes = 0, // To be calculated
          .base_offset = header.base_offset,
          .type = header.type,
          .crc = 0, // To be calculated
          .attrs = model::record_batch_attributes(0),
          .last_offset_delta = header.last_offset_delta,
          .first_timestamp = header.first_timestamp,
          .max_timestamp = header.max_timestamp,
          .producer_id = header.producer_id,
          .producer_epoch = header.producer_epoch,
          .base_sequence = header.base_sequence,
          .record_count = 0, // To be calculated
          .ctx = model::record_batch_header::context(
            header.ctx.term, ss::this_shard_id()),
        };

        // In the case of an async host call, don't allow multiple
        // calls into the wasm engine concurrently (I think there
        // are mutex in wasmedge that would deadlock for us).
        auto holder = _mutex.get_units();

        if (!_wasi_started) {
            co_await ss::async([this] { initialize_wasi(); });
            _wasi_started = true;
        }

        // TODO: Put in a scheduling group
        co_await ss::async(
          [this, &transformed_records](model::record_batch decompressed) {
              decompressed.for_each_record(
                [this, &transformed_records, &decompressed](
                  model::record record) {
                    auto output = invoke_transform(
                      decompressed.header(), std::move(record));
                    transformed_records.insert(
                      transformed_records.end(),
                      std::make_move_iterator(output.begin()),
                      std::make_move_iterator(output.end()));
                });
          },
          std::move(decompressed));

        auto batch_size = model::packed_record_batch_header_size;
        for (const auto& r : transformed_records) {
            batch_size += vint::vint_size(r.size_bytes());
            batch_size += r.size_bytes();
        }
        new_header.size_bytes = batch_size;
        new_header.record_count = transformed_records.size();
        auto transformed_batch = model::record_batch(
          new_header, std::move(transformed_records));
        transformed_batch.header().crc = model::crc_record_batch(
          transformed_batch);
        transformed_batch.header().header_crc = model::internal_header_only_crc(
          transformed_batch.header());
        co_return std::move(transformed_batch);
    }

private:
    wasmedge_wasm_engine()
      : engine()
      , _store_ctx(nullptr, [](auto) {})
      , _vm_ctx(nullptr, [](auto) {}){};

    void initialize(
      WasmEdgeVM vm_ctx,
      WasmEdgeStore store_ctx,
      std::vector<WasmEdgeModule> modules) {
        _vm_ctx = std::move(vm_ctx);
        _store_ctx = std::move(store_ctx);
        _modules = std::move(modules);
    }

    void initialize_wasi() {
        std::array<WasmEdge_Value, 0> params = {};
        std::array<WasmEdge_Value, 0> returns = {};
        WasmEdge_Result result = WasmEdge_VMExecute(
          _vm_ctx.get(),
          WasmEdge_StringWrap(
            wasi_preview_1_start_function_name.data(),
            wasi_preview_1_start_function_name.size()),
          params.data(),
          params.size(),
          returns.data(),
          returns.size());

        if (!WasmEdge_ResultOK(result)) {
            // Get the right transform name here
            std::string_view user_transform_name = "foo";
            throw wasm_exception(ss::format(
              "wasi _start initialization {} failed", user_transform_name));
        }
    }

    std::vector<model::record> invoke_transform(
      const model::record_batch_header& header, model::record&& record) {
        iobuf key = record.release_key();
        iobuf value = record.release_value();
        _call_ctx.emplace(transform_context{
          .key = iobuf::iterator_consumer(key.cbegin(), key.cend()),
          .value = iobuf::iterator_consumer(value.cbegin(), value.cend()),
          .offset = model::offset(header.base_offset() + record.offset_delta()),
          .timestamp = model::timestamp(
            header.first_timestamp() + record.timestamp_delta()),
          .headers = std::exchange(record.headers(), {}),
        });
        std::array<WasmEdge_Value, 1> params = {
          WasmEdge_ValueGenI32(fixed_input_record_handle)};
        std::array<WasmEdge_Value, 1> returns = {WasmEdge_ValueGenI32(-1)};
        WasmEdge_Result result = WasmEdge_VMExecute(
          _vm_ctx.get(),
          WasmEdge_StringWrap(
            redpanda_on_record_callback_function_name.data(),
            redpanda_on_record_callback_function_name.size()),
          params.data(),
          params.size(),
          returns.data(),
          returns.size());
        // Get the right transform name here
        std::string_view user_transform_name = "foo";
        if (!WasmEdge_ResultOK(result)) {
            _call_ctx = std::nullopt;
            throw wasm_exception(
              ss::format("transform execution {} failed", user_transform_name));
        }
        auto user_result = WasmEdge_ValueGetI32(returns[0]);
        if (user_result != 0) {
            _call_ctx = std::nullopt;
            throw wasm_exception(ss::format(
              "transform execution {} resulted in error {}",
              user_transform_name,
              user_result));
        }
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
                        + vint::vint_size(record.timestamp_delta())
                        + vint::vint_size(record.offset_delta())
                        + vint::vint_size(k_size) + std::min(k_size, 0)
                        + vint::vint_size(v_size) + std::min(v_size, 0)
                        + vint::vint_size(output_record.headers.size());
            for (auto& h : output_record.headers) {
                size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                        + vint::vint_size(h.value_size())
                        + h.value().size_bytes();
            }
            auto r = model::record(
              size,
              record.attributes(),
              record.timestamp_delta(),
              record.offset_delta(),
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

    std::optional<transform_context> _call_ctx;

    // Start ABI exports
    // This is a small set just to get the ball rolling

    read_result read_key(input_record_handle handle, ffi::array<uint8_t> data) {
        if (handle != fixed_input_record_handle || !_call_ctx || !data) {
            return -1;
        }
        size_t remaining = _call_ctx->key.segment_bytes_left();
        size_t amount = std::min(size_t(data.size()), remaining);
        _call_ctx->key.consume_to(amount, data.raw());
        return int32_t(amount);
    }

    read_result
    read_value(input_record_handle handle, ffi::array<uint8_t> data) {
        if (handle != fixed_input_record_handle || !_call_ctx || !data) {
            return -1;
        }
        size_t remaining = _call_ctx->value.segment_bytes_left();
        size_t amount = std::min(size_t(data.size()), remaining);
        _call_ctx->value.consume_to(amount, data.raw());
        return int32_t(amount);
    }

    output_record_handle create_output_record() {
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

    write_result
    write_key(output_record_handle handle, ffi::array<uint8_t> data) {
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

    write_result
    write_value(output_record_handle handle, ffi::array<uint8_t> data) {
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

    int32_t num_headers(input_record_handle handle) {
        if (!_call_ctx || handle != fixed_input_record_handle) {
            return -1;
        }
        return int32_t(_call_ctx->headers.size());
    }

    int32_t
    find_header_by_key(input_record_handle handle, ffi::array<uint8_t> key) {
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

    int32_t get_header_key_length(input_record_handle handle, int32_t index) {
        if (
          !_call_ctx || handle != fixed_input_record_handle || index < 0
          || index >= int32_t(_call_ctx->headers.size())) {
            return -1;
        }
        return int32_t(_call_ctx->headers[index].key_size());
    }

    int32_t get_header_value_length(input_record_handle handle, int32_t index) {
        if (
          !_call_ctx || handle != fixed_input_record_handle || index < 0
          || index >= int32_t(_call_ctx->headers.size())) {
            return -1;
        }
        return int32_t(_call_ctx->headers[index].value_size());
    }

    int32_t get_header_key(
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

    int32_t get_header_value(
      input_record_handle handle, int32_t index, ffi::array<uint8_t> value) {
        if (
          !_call_ctx || !value || handle != fixed_input_record_handle
          || index < 0 || index >= int32_t(_call_ctx->headers.size())) {
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

    int32_t append_header(
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

    int32_t http_fetch(
      uint32_t method,
      ffi::array<uint8_t> url,
      ffi::array<uint8_t> headers,
      ffi::array<uint8_t> body) {
        try {
            if (!_call_ctx) {
                return -1;
            }

            auto uri = util::parse_uri(ffi::array_as_string_view(url));

            if (!uri.has_value()) {
                return -2;
            }
            std::string_view headers_str = ffi::array_as_string_view(headers);
            std::string_view body_str = ffi::array_as_string_view(body);
            net::base_transport::configuration client_config{
              .server_addr = net::unresolved_address(
                // TODO: Respect the passed in port if set
                uri->host,
                uri->scheme == "http" ? 80 : 443),
            };
            if (uri->scheme == "https") {
                ss::tls::credentials_builder b;
                b.set_client_auth(ss::tls::client_auth::NONE);
                auto ca_file = net::find_ca_file().get();
                vlog(
                  wasm_log.info,
                  "Using ca_file: {}",
                  ca_file.value_or("system"));
                if (ca_file.has_value()) {
                    b.set_x509_trust_file(
                       ca_file.value(), ss::tls::x509_crt_format::PEM)
                      .get();
                } else {
                    b.set_system_trust().get();
                }
                client_config.credentials
                  = b.build_reloadable_certificate_credentials().get();
                client_config.tls_sni_hostname = uri->host;
            }

            http::client client(client_config);

            return http::with_client(
                     std::move(client),
                     [uri = std::move(uri.value()),
                      headers_str,
                      body_str,
                      method,
                      this](auto& client) mutable {
                         return make_http_request(
                           client,
                           method == 0 ? boost::beast::http::verb::get
                                       : boost::beast::http::verb::post,
                           std::move(uri),
                           headers_str,
                           body_str);
                     })
              .get();

        } catch (...) {
            vlog(
              wasm_log.warn,
              "Error making HTTP request: {}",
              std::current_exception());
            return -3;
        }
    }

    ss::future<int32_t> make_http_request(
      http::client& client,
      boost::beast::http::verb method,
      util::uri uri,
      std::string_view headers,
      std::string_view body) {
        http::client::request_header header;
        header.method(method);
        header.target(std::string(uri.path + "?" + uri.query));

        if (!headers.empty()) {
            for (auto header_line : absl::StrSplit(headers, '\n')) {
                std::vector<std::string_view> parts = absl::StrSplit(
                  header_line, absl::MaxSplits(": ", 1));
                if (parts.size() != 2) {
                    continue;
                }
                boost::beast::string_view name(
                  parts[0].data(), parts[0].length());
                boost::beast::string_view value(
                  parts[1].data(), parts[1].length());

                header.insert(
                  boost::beast::http::string_to_field(name), name, value);
            }
        }

        header.insert(
          boost::beast::http::field::content_length,
          boost::beast::to_static_string(body.length()));
        header.insert(boost::beast::http::field::host, std::string(uri.host));

        vlog(wasm_log.info, "sending fetch: {}", header);

        auto [req, resp] = co_await client.make_request(std::move(header));

        co_await req->send_some(ss::temporary_buffer<char>::copy_of(body));
        co_await req->send_eof();

        iobuf result;
        while (!resp->is_done()) {
            result.append_fragments(co_await resp->recv_some());
        }
        auto idx = _call_ctx->http_responses.size();
        _call_ctx->http_responses.emplace_back(std::move(result));
        co_return int32_t(idx);
    }

    int32_t read_http_resp_body(int32_t handle, ffi::array<uint8_t> buf) {
        if (
          !_call_ctx || !buf || handle < 0
          || handle >= int32_t(_call_ctx->http_responses.size())) {
            return -1;
        }
        auto& resp = _call_ctx->http_responses[handle];
        auto amt = std::min(buf.size(), uint32_t(resp.bytes_left()));
        resp.consume_to(amt, buf.raw());
        return amt;
    }

    // End ABI exports

    mutex _mutex;
    std::vector<WasmEdgeModule> _modules;
    WasmEdgeStore _store_ctx;
    WasmEdgeVM _vm_ctx;
    bool _wasi_started = false;
};

namespace wasi {

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L110-L113C1
constexpr uint16_t WASI_ERRNO_SUCCESS = 0;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L250-L253C1
constexpr uint16_t WASI_ERRNO_INVAL = 16;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr uint16_t WASI_ERRNO_NOSYS = 52;
//
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr uint16_t WASI_ERRNO_BADF = 8;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1453-L1469
uint16_t clock_time_get(uint32_t, uint64_t, uint64_t) {
    return WASI_ERRNO_NOSYS;
}

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1409-L1418C1
int32_t args_sizes_get(uint32_t*, uint32_t*) { return 0; }

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1400-L1408
int16_t args_get(uint8_t**, uint8_t*) { return WASI_ERRNO_SUCCESS; }

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1419-L1427
int32_t environ_get(uint8_t**, uint8_t*) { return WASI_ERRNO_SUCCESS; }

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1428-L1437
int32_t environ_sizes_get(uint32_t*, uint32_t*) { return 0; }
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
int16_t fd_close(int32_t) { return WASI_ERRNO_NOSYS; }
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1518-L1527
int16_t fd_fdstat_get(int32_t, void*) { return WASI_ERRNO_NOSYS; }
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1612-L1620
int16_t fd_prestat_get(int32_t fd, void*) {
    if (fd == 0 || fd == 1 || fd == 2) {
        // stdin, stdout, stderr are fine but unimplemented
        return WASI_ERRNO_NOSYS;
    }
    // We don't hand out any file descriptors and this is needed for wasi_libc
    return WASI_ERRNO_BADF;
}
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1621-L1631
int16_t fd_prestat_dir_name(int32_t, uint8_t*, uint32_t) {
    return WASI_ERRNO_NOSYS;
}
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1654-L1671
int16_t fd_read(int32_t, const void*, uint32_t, uint32_t*) {
    return WASI_ERRNO_NOSYS;
}

/**
 * A region of memory for scatter/gather writes.
 */
struct ciovec_t {
    /** The address of the buffer to be written. */
    uint32_t buf;
    /** The length of the buffer to be written. */
    uint32_t buf_len;
};

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1750-L1765
int16_t fd_write(
  WasmEdge_MemoryInstanceContext* mem,
  int32_t fd,
  ffi::array<ciovec_t> iovecs,
  uint32_t* written) {
    if (written == nullptr || !iovecs) [[unlikely]] {
        return WASI_ERRNO_INVAL;
    }
    if (fd == 1) {
        std::stringstream ss;
        for (unsigned i = 0; i < iovecs.size(); ++i) {
            const auto& vec = iovecs[i];
            const uint8_t* data = WasmEdge_MemoryInstanceGetPointerConst(
              mem, vec.buf, vec.buf_len);
            if (data == nullptr) [[unlikely]] {
                return WASI_ERRNO_INVAL;
            }
            ss << std::string_view(
              reinterpret_cast<const char*>(data), vec.buf_len);
        }
        // TODO: We should be buffering these until a newline or something and
        // emitting logs line by line Also: rate limit logs
        auto str = ss.str();
        wasm_log.info("Guest stdout: {}", str);
        *written = str.size();
        return WASI_ERRNO_SUCCESS;
    }
    return WASI_ERRNO_NOSYS;
}
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1715-L1732
int16_t fd_seek(int32_t, int32_t) { return WASI_ERRNO_NOSYS; }
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1845-L1884
int16_t path_open(
  int32_t,
  uint32_t,
  const uint8_t*,
  uint16_t,
  uint64_t,
  uint64_t,
  uint16_t,
  void*) {
    return WASI_ERRNO_NOSYS;
}
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1982-L1992
void proc_exit(int32_t exit_code) {
    throw std::runtime_error(ss::format("Exiting: {}", exit_code));
}

template<typename ResultType, typename... ArgTypes>
errc register_function(
  const WasmEdgeModule& mod,
  ResultType (*host_fn)(ArgTypes...),
  std::string_view function_name) {
    std::vector<WasmEdge_ValType> inputs;
    ffi::transform_types<ArgTypes...>(inputs);
    std::vector<WasmEdge_ValType> outputs;
    ffi::transform_type<ResultType>(outputs);
    auto func_type_ctx = WasmEdgeFuncType(
      WasmEdge_FunctionTypeCreate(
        inputs.data(), inputs.size(), outputs.data(), outputs.size()),
      &WasmEdge_FunctionTypeDelete);
    if (!func_type_ctx.get()) {
        vlog(
          wasm_log.warn,
          "Failed to register host function types: {}",
          function_name);
        return errc::load_failure;
    }
    WasmEdge_FunctionInstanceContext* func = WasmEdge_FunctionInstanceCreate(
      func_type_ctx.get(),
      [](
        void* data,
        const WasmEdge_CallingFrameContext* calling_ctx,
        const WasmEdge_Value* guest_params,
        WasmEdge_Value* guest_results) {
          auto host_fn = reinterpret_cast<ResultType (*)(ArgTypes...)>(data);
          auto host_params = ffi::extract_parameters<ArgTypes...>(
            calling_ctx, guest_params, 0);
          try {
              if constexpr (std::is_same_v<ResultType, void>) {
                  std::apply(host_fn, std::move(host_params));
              } else {
                  auto host_result = std::apply(
                    host_fn, std::move(host_params));
                  ffi::pack_result(guest_results, host_result);
              }
          } catch (...) {
              vlog(
                wasm_log.warn,
                "Error executing host wasi function: {}",
                std::current_exception());
              // TODO: When do we fail vs terminate?
              return WasmEdge_Result_Terminate;
          }
          return WasmEdge_Result_Success;
      },
      reinterpret_cast<void*>(host_fn),
      0);
    if (!func) {
        vlog(
          wasm_log.warn, "Failed to register host function: {}", function_name);
        return errc::load_failure;
    }
    WasmEdge_ModuleInstanceAddFunction(
      mod.get(),
      WasmEdge_StringWrap(function_name.data(), function_name.size()),
      func);
    return errc::success;
}

} // namespace wasi

// TODO: Unify this with the above wasi host function registeration
template<auto value>
struct host_function;
template<
  typename ReturnType,
  typename... ArgTypes,
  ReturnType (wasmedge_wasm_engine::*engine_func)(ArgTypes...)>
struct host_function<engine_func> {
    static errc reg(
      const std::unique_ptr<wasmedge_wasm_engine>& engine,
      const WasmEdgeModule& mod,
      std::string_view function_name) {
        std::vector<WasmEdge_ValType> inputs;
        ffi::transform_types<ArgTypes...>(inputs);
        std::vector<WasmEdge_ValType> outputs;
        ffi::transform_types<ReturnType>(outputs);
        auto func_type_ctx = WasmEdgeFuncType(
          WasmEdge_FunctionTypeCreate(
            inputs.data(), inputs.size(), outputs.data(), outputs.size()),
          &WasmEdge_FunctionTypeDelete);

        if (!func_type_ctx) {
            vlog(
              wasm_log.warn,
              "Failed to register host function: {}",
              function_name);
            return errc::load_failure;
        }

        if (!func_type_ctx.get()) {
            vlog(
              wasm_log.warn,
              "Failed to register host function types: {}",
              function_name);
            return errc::load_failure;
        }

        WasmEdge_FunctionInstanceContext* func
          = WasmEdge_FunctionInstanceCreate(
            func_type_ctx.get(),
            [](
              void* data,
              const WasmEdge_CallingFrameContext* calling_ctx,
              const WasmEdge_Value* guest_params,
              WasmEdge_Value* guest_returns) {
                auto engine = static_cast<wasmedge_wasm_engine*>(data);
                auto host_params = ffi::extract_parameters<ArgTypes...>(
                  calling_ctx, guest_params, 0);
                try {
                    ReturnType host_result = std::apply(
                      engine_func,
                      std::tuple_cat(std::make_tuple(engine), host_params));
                    ffi::pack_result(guest_returns, host_result);
                } catch (...) {
                    vlog(
                      wasm_log.warn,
                      "Error executing engine function: {}",
                      std::current_exception());
                    // TODO: When do we fail vs terminate?
                    return WasmEdge_Result_Terminate;
                }
                return WasmEdge_Result_Success;
            },
            static_cast<void*>(engine.get()),
            /*cost=*/0);

        if (!func) {
            vlog(
              wasm_log.warn,
              "Failed to register host function: {}",
              function_name);
            return errc::load_failure;
        }
        WasmEdge_ModuleInstanceAddFunction(
          mod.get(),
          WasmEdge_StringWrap(function_name.data(), function_name.size()),
          func);

        return errc::success;
    }
};

result<std::unique_ptr<wasmedge_wasm_engine>, errc>
wasmedge_wasm_engine::create(std::string_view module_source) {
    auto config_ctx = WasmEdgeConfig(
      WasmEdge_ConfigureCreate(), &WasmEdge_ConfigureDelete);

    auto store_ctx = WasmEdgeStore(
      WasmEdge_StoreCreate(), &WasmEdge_StoreDelete);

    auto vm_ctx = WasmEdgeVM(
      WasmEdge_VMCreate(config_ctx.get(), store_ctx.get()), &WasmEdge_VMDelete);

    auto engine = std::unique_ptr<wasmedge_wasm_engine>(
      new wasmedge_wasm_engine());

    WasmEdge_Result result;

    auto redpanda_module = create_module(redpanda_module_name);

    host_function<&wasmedge_wasm_engine::read_key>::reg(
      engine, redpanda_module, "read_key");
    host_function<&wasmedge_wasm_engine::read_value>::reg(
      engine, redpanda_module, "read_value");
    host_function<&wasmedge_wasm_engine::create_output_record>::reg(
      engine, redpanda_module, "create_output_record");
    host_function<&wasmedge_wasm_engine::write_key>::reg(
      engine, redpanda_module, "write_key");
    host_function<&wasmedge_wasm_engine::write_value>::reg(
      engine, redpanda_module, "write_value");
    host_function<&wasmedge_wasm_engine::num_headers>::reg(
      engine, redpanda_module, "num_headers");
    host_function<&wasmedge_wasm_engine::find_header_by_key>::reg(
      engine, redpanda_module, "find_header_by_key");
    host_function<&wasmedge_wasm_engine::get_header_key_length>::reg(
      engine, redpanda_module, "get_header_key_length");
    host_function<&wasmedge_wasm_engine::get_header_key>::reg(
      engine, redpanda_module, "get_header_key");
    host_function<&wasmedge_wasm_engine::get_header_value_length>::reg(
      engine, redpanda_module, "get_header_value_length");
    host_function<&wasmedge_wasm_engine::get_header_value>::reg(
      engine, redpanda_module, "get_header_value");
    host_function<&wasmedge_wasm_engine::append_header>::reg(
      engine, redpanda_module, "append_header");
    host_function<&wasmedge_wasm_engine::http_fetch>::reg(
      engine, redpanda_module, "http_fetch");
    host_function<&wasmedge_wasm_engine::read_http_resp_body>::reg(
      engine, redpanda_module, "read_http_resp_body");

    WasmEdge_VMRegisterModuleFromImport(vm_ctx.get(), redpanda_module.get());

    auto wasi1_module = create_module(wasi_preview_1_module_name);

    wasi::register_function(
      wasi1_module, wasi::clock_time_get, "clock_time_get");
    wasi::register_function(
      wasi1_module, wasi::args_sizes_get, "args_sizes_get");
    wasi::register_function(wasi1_module, wasi::args_get, "args_get");
    wasi::register_function(wasi1_module, wasi::environ_get, "environ_get");
    wasi::register_function(
      wasi1_module, wasi::environ_sizes_get, "environ_sizes_get");
    wasi::register_function(wasi1_module, wasi::fd_close, "fd_close");
    wasi::register_function(wasi1_module, wasi::fd_fdstat_get, "fd_fdstat_get");
    wasi::register_function(
      wasi1_module, wasi::fd_prestat_get, "fd_prestat_get");
    wasi::register_function(
      wasi1_module, wasi::fd_prestat_dir_name, "fd_prestat_dir_name");
    wasi::register_function(wasi1_module, wasi::fd_read, "fd_read");
    wasi::register_function(wasi1_module, wasi::fd_write, "fd_write");
    wasi::register_function(wasi1_module, wasi::fd_seek, "fd_seek");
    wasi::register_function(wasi1_module, wasi::path_open, "path_open");
    wasi::register_function(wasi1_module, wasi::proc_exit, "proc_exit");

    WasmEdge_VMRegisterModuleFromImport(vm_ctx.get(), wasi1_module.get());

    auto loader_ctx = WasmEdgeLoader(
      WasmEdge_LoaderCreate(config_ctx.get()), &WasmEdge_LoaderDelete);

    WasmEdge_ASTModuleContext* module_ctx_ptr = nullptr;
    result = WasmEdge_LoaderParseFromBuffer(
      loader_ctx.get(),
      &module_ctx_ptr,
      reinterpret_cast<const uint8_t*>(module_source.data()),
      module_source.size());
    auto module_ctx = WasmEdgeASTModule(
      module_ctx_ptr, &WasmEdge_ASTModuleDelete);

    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasm_log.warn,
          "Failed to load module: {}",
          WasmEdge_ResultGetMessage(result));
        return errc::load_failure;
    }

    result = WasmEdge_VMLoadWasmFromASTModule(vm_ctx.get(), module_ctx.get());

    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasm_log.warn,
          "Failed to load module: {}",
          WasmEdge_ResultGetMessage(result));
        return errc::load_failure;
    }

    result = WasmEdge_VMValidate(vm_ctx.get());
    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasm_log.warn,
          "Failed to create engine: {}",
          WasmEdge_ResultGetMessage(result));
        return errc::engine_creation_failure;
    }

    result = WasmEdge_VMInstantiate(vm_ctx.get());
    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasm_log.warn,
          "Failed to create engine: {}",
          WasmEdge_ResultGetMessage(result));
        return errc::engine_creation_failure;
    }

    std::vector<WasmEdgeModule> modules;
    modules.push_back(std::move(redpanda_module));
    modules.push_back(std::move(wasi1_module));

    engine->initialize(
      std::move(vm_ctx), std::move(store_ctx), std::move(modules));

    return std::move(engine);
}

class wasm_transform_applying_reader : public model::record_batch_reader::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    wasm_transform_applying_reader(
      model::record_batch_reader r,
      engine* engine,
      ss::gate::holder gate_holder)
      : _gate_holder(std::move(gate_holder))
      , _engine(engine)
      , _source(std::move(r).release()) {}

    bool is_end_of_stream() const final { return _source->is_end_of_stream(); }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point tout) final {
        storage_t ret = co_await _source->do_load_slice(tout);
        data_t output;
        if (std::holds_alternative<data_t>(ret)) {
            auto& d = std::get<data_t>(ret);
            output.reserve(d.size());
            for (auto& batch : d) {
                auto transformed = co_await _engine->transform(
                  std::move(batch));
                output.emplace_back(std::move(transformed));
            }
        } else {
            auto& d = std::get<foreign_data_t>(ret);
            for (auto& batch : *d.buffer) {
                auto transformed = co_await _engine->transform(
                  std::move(batch));
                output.emplace_back(std::move(transformed));
            }
        }
        co_return std::move(output);
    }

    void print(std::ostream& os) final {
        fmt::print(os, "{wasm transform applying reader}");
    }

private:
    ss::gate::holder _gate_holder;
    engine* _engine;
    std::unique_ptr<model::record_batch_reader::impl> _source;
};

} // namespace

service::service(std::unique_ptr<engine> engine)
  : _engine(std::move(engine)) {}

ss::future<> service::stop() { return _gate.close(); }

model::record_batch_reader
service::wrap_batch_reader(model::record_batch_reader batch_reader) {
    if (is_enabled()) {
        return model::make_record_batch_reader<wasm_transform_applying_reader>(
          std::move(batch_reader), _engine.get(), _gate.hold());
    }
    return batch_reader;
}

result<std::unique_ptr<engine>, errc>
make_wasm_engine(std::string_view wasm_source) {
    auto r = wasmedge_wasm_engine::create(wasm_source);
    if (r.has_error()) {
        return r.error();
    }
    return std::move(r.value());
}

} // namespace wasm
