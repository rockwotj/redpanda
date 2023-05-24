#include "seastarx.h"
#include "storage/parser_utils.h"
#include "utils/mutex.h"
#include "wasm/ffi.h"
#include "wasm/probe.h"
#include "wasm/wasi.h"
#include "wasm/wasm.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

#include <wasmedge/enum_types.h>
#include <wasmedge/wasmedge.h>

#include <memory>
#include <type_traits>

namespace wasm::wasmedge {

static ss::logger wasmedge_log("wasmedge");

using read_result = int32_t;
using write_result = int32_t;
using input_record_handle = int32_t;
using output_record_handle = int32_t;

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;
constexpr size_t max_output_records = 256;
constexpr std::string_view redpanda_module_name = "redpanda";
constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";

template<class T>
struct dependent_false : std::false_type {};

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

class memory : public ffi::memory {
public:
    explicit memory(WasmEdge_MemoryInstanceContext* mem)
      : ffi::memory()
      , _underlying(mem) {}

    void* translate(size_t guest_ptr, size_t len) final {
        return WasmEdge_MemoryInstanceGetPointer(_underlying, guest_ptr, len);
    }

private:
    WasmEdge_MemoryInstanceContext* _underlying;
};

class wasmedge_wasm_engine;

template<typename Type>
void pack_result(WasmEdge_Value* results, Type result) {
    if constexpr (ss::is_future<Type>::value) {
        pack_result(results, result.get0());
    } else if constexpr (
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

    std::vector<record_builder> output_records;
};

class wasmedge_wasm_engine : public engine {
public:
    static ss::future<std::unique_ptr<wasmedge_wasm_engine>>
    create(std::string_view module_name, std::string_view module_binary);

    ss::future<model::record_batch>
    transform(model::record_batch&& batch, probe* probe) override {
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
          [this, &transformed_records, probe](
            model::record_batch decompressed) {
              decompressed.for_each_record(
                [this, &transformed_records, &decompressed, probe](
                  model::record record) {
                    auto output = invoke_transform(
                      decompressed.header(), std::move(record), probe);
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
    wasmedge_wasm_engine(ss::sstring name)
      : engine()
      , _user_module_name(std::move(name))
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
            wasi::preview_1_start_function_name.data(),
            wasi::preview_1_start_function_name.size()),
          params.data(),
          params.size(),
          returns.data(),
          returns.size());

        if (!WasmEdge_ResultOK(result)) {
            // Get the right transform name here
            throw wasm_exception(
              ss::format(
                "wasi _start initialization {} failed", _user_module_name),
              errc::user_code_failure);
        }
    }

    std::vector<model::record> invoke_transform(
      const model::record_batch_header& header,
      model::record&& record,
      probe* probe) {
        auto m = probe->auto_transform_measurement();
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
        probe->transform_complete();
        // Get the right transform name here
        if (!WasmEdge_ResultOK(result)) {
            _call_ctx = std::nullopt;
            probe->transform_error();
            throw wasm_exception(
              ss::format("transform execution {} failed", _user_module_name),
              errc::user_code_failure);
        }
        auto user_result = WasmEdge_ValueGetI32(returns[0]);
        if (user_result != 0) {
            _call_ctx = std::nullopt;
            probe->transform_error();
            throw wasm_exception(
              ss::format(
                "transform execution {} resulted in error {}",
                _user_module_name,
                user_result),
              errc::user_code_failure);
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
                        + vint::vint_size(k_size) + std::max(k_size, 0)
                        + vint::vint_size(v_size) + std::max(v_size, 0)
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

    // End ABI exports

    ss::sstring _user_module_name;
    mutex _mutex;
    std::vector<WasmEdgeModule> _modules;
    WasmEdgeStore _store_ctx;
    WasmEdgeVM _vm_ctx;
    bool _wasi_started = false;
};

std::vector<WasmEdge_ValType>
convert_to_wasmedge(const std::vector<ffi::val_type>& ffi_types) {
    std::vector<WasmEdge_ValType> wasmedge_types;
    wasmedge_types.reserve(ffi_types.size());
    for (auto ty : ffi_types) {
        switch (ty) {
        case ffi::val_type::i32:
            wasmedge_types.push_back(WasmEdge_ValType_I32);
            break;
        case ffi::val_type::i64:
            wasmedge_types.push_back(WasmEdge_ValType_I64);
            break;
        case ffi::val_type::f32:
            wasmedge_types.push_back(WasmEdge_ValType_F32);
            break;
        case ffi::val_type::f64:
            wasmedge_types.push_back(WasmEdge_ValType_F64);
            break;
        }
    }
    return wasmedge_types;
}

template<typename ResultType, typename... ArgTypes>
errc register_wasi_function(
  const WasmEdgeModule& mod,
  ResultType (*host_fn)(ArgTypes...),
  std::string_view function_name) {
    std::vector<ffi::val_type> ffi_inputs;
    ffi::transform_types<ArgTypes...>(ffi_inputs);
    std::vector<ffi::val_type> ffi_outputs;
    ffi::transform_types<ResultType>(ffi_outputs);
    std::vector<WasmEdge_ValType> inputs = convert_to_wasmedge(ffi_inputs);
    std::vector<WasmEdge_ValType> outputs = convert_to_wasmedge(ffi_outputs);
    auto func_type_ctx = WasmEdgeFuncType(
      WasmEdge_FunctionTypeCreate(
        inputs.data(), inputs.size(), outputs.data(), outputs.size()),
      &WasmEdge_FunctionTypeDelete);
    if (!func_type_ctx.get()) {
        vlog(
          wasmedge_log.warn,
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
          auto mem = memory(
            WasmEdge_CallingFrameGetMemoryInstance(calling_ctx, 0));
          std::vector<uint64_t> raw_params;
          raw_params.reserve(sizeof...(ArgTypes));
          for (size_t i = 0; i < sizeof...(ArgTypes); ++i) {
              raw_params.push_back(WasmEdge_ValueGetI64(guest_params[i]));
          }
          auto host_params = extract_parameters<ArgTypes...>(
            &mem, raw_params, 0);
          try {
              if constexpr (std::is_same_v<ResultType, ss::future<>>) {
                  std::apply(host_fn, std::move(host_params)).get();
              } else if constexpr (std::is_same_v<ResultType, void>) {
                  std::apply(host_fn, std::move(host_params));
              } else {
                  auto host_result = std::apply(
                    host_fn, std::move(host_params));
                  pack_result(guest_results, host_result);
              }
          } catch (...) {
              vlog(
                wasmedge_log.warn,
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
          wasmedge_log.warn,
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
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        std::vector<WasmEdge_ValType> inputs = convert_to_wasmedge(
          std::move(ffi_inputs));
        std::vector<WasmEdge_ValType> outputs = convert_to_wasmedge(
          std::move(ffi_outputs));
        auto func_type_ctx = WasmEdgeFuncType(
          WasmEdge_FunctionTypeCreate(
            inputs.data(), inputs.size(), outputs.data(), outputs.size()),
          &WasmEdge_FunctionTypeDelete);

        if (!func_type_ctx) {
            vlog(
              wasmedge_log.warn,
              "Failed to register host function: {}",
              function_name);
            return errc::load_failure;
        }

        if (!func_type_ctx.get()) {
            vlog(
              wasmedge_log.warn,
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
                auto mem = memory(
                  WasmEdge_CallingFrameGetMemoryInstance(calling_ctx, 0));
                std::vector<uint64_t> raw_params;
                raw_params.reserve(sizeof...(ArgTypes));
                for (size_t i = 0; i < sizeof...(ArgTypes); ++i) {
                    raw_params.push_back(WasmEdge_ValueGetI64(guest_params[i]));
                }
                auto host_params = ffi::extract_parameters<ArgTypes...>(
                  &mem, raw_params, 0);
                try {
                    ReturnType host_result = std::apply(
                      engine_func,
                      std::tuple_cat(std::make_tuple(engine), host_params));
                    pack_result(guest_returns, std::move(host_result));
                } catch (...) {
                    vlog(
                      wasmedge_log.warn,
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
              wasmedge_log.warn,
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

ss::future<std::unique_ptr<wasmedge_wasm_engine>> wasmedge_wasm_engine::create(
  std::string_view module_name, std::string_view module_binary) {
    auto config_ctx = WasmEdgeConfig(
      WasmEdge_ConfigureCreate(), &WasmEdge_ConfigureDelete);

    auto store_ctx = WasmEdgeStore(
      WasmEdge_StoreCreate(), &WasmEdge_StoreDelete);

    auto vm_ctx = WasmEdgeVM(
      WasmEdge_VMCreate(config_ctx.get(), store_ctx.get()), &WasmEdge_VMDelete);

    auto engine = std::unique_ptr<wasmedge_wasm_engine>(
      new wasmedge_wasm_engine(ss::sstring(module_name)));

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

    WasmEdge_VMRegisterModuleFromImport(vm_ctx.get(), redpanda_module.get());

    auto wasi1_module = create_module(wasi::preview_1_module_name);

    register_wasi_function(
      wasi1_module, wasi::clock_time_get, "clock_time_get");
    register_wasi_function(
      wasi1_module, wasi::args_sizes_get, "args_sizes_get");
    register_wasi_function(wasi1_module, wasi::args_get, "args_get");
    register_wasi_function(wasi1_module, wasi::environ_get, "environ_get");
    register_wasi_function(
      wasi1_module, wasi::environ_sizes_get, "environ_sizes_get");
    register_wasi_function(wasi1_module, wasi::fd_close, "fd_close");
    register_wasi_function(wasi1_module, wasi::fd_fdstat_get, "fd_fdstat_get");
    register_wasi_function(
      wasi1_module, wasi::fd_prestat_get, "fd_prestat_get");
    register_wasi_function(
      wasi1_module, wasi::fd_prestat_dir_name, "fd_prestat_dir_name");
    register_wasi_function(wasi1_module, wasi::fd_read, "fd_read");
    register_wasi_function(wasi1_module, wasi::fd_write, "fd_write");
    register_wasi_function(wasi1_module, wasi::fd_seek, "fd_seek");
    register_wasi_function(wasi1_module, wasi::path_open, "path_open");
    register_wasi_function(wasi1_module, wasi::proc_exit, "proc_exit");
    register_wasi_function(wasi1_module, wasi::sched_yield, "sched_yield");

    WasmEdge_VMRegisterModuleFromImport(vm_ctx.get(), wasi1_module.get());

    auto loader_ctx = WasmEdgeLoader(
      WasmEdge_LoaderCreate(config_ctx.get()), &WasmEdge_LoaderDelete);

    WasmEdge_ASTModuleContext* module_ctx_ptr = nullptr;
    result = WasmEdge_LoaderParseFromBuffer(
      loader_ctx.get(),
      &module_ctx_ptr,
      reinterpret_cast<const uint8_t*>(module_binary.data()),
      module_binary.size());
    auto module_ctx = WasmEdgeASTModule(
      module_ctx_ptr, &WasmEdge_ASTModuleDelete);

    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasmedge_log.warn,
          "Failed to load module: {}",
          WasmEdge_ResultGetMessage(result));
        throw wasm_exception(
          ss::format(
            "Failed to load module: {}", WasmEdge_ResultGetMessage(result)),
          errc::load_failure);
    }

    result = WasmEdge_VMLoadWasmFromASTModule(vm_ctx.get(), module_ctx.get());

    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasmedge_log.warn,
          "Failed to load module: {}",
          WasmEdge_ResultGetMessage(result));
        throw wasm_exception(
          ss::format(
            "Failed to load module: {}", WasmEdge_ResultGetMessage(result)),
          errc::load_failure);
    }

    result = WasmEdge_VMValidate(vm_ctx.get());
    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasmedge_log.warn,
          "Failed to create engine: {}",
          WasmEdge_ResultGetMessage(result));
        throw wasm_exception(
          ss::format(
            "Failed to create engine: {}", WasmEdge_ResultGetMessage(result)),
          errc::engine_creation_failure);
    }

    result = WasmEdge_VMInstantiate(vm_ctx.get());
    if (!WasmEdge_ResultOK(result)) {
        vlog(
          wasmedge_log.warn,
          "Failed to create engine: {}",
          WasmEdge_ResultGetMessage(result));
        throw wasm_exception(
          ss::format(
            "Failed to create engine: {}", WasmEdge_ResultGetMessage(result)),
          errc::engine_creation_failure);
    }

    std::vector<WasmEdgeModule> modules;
    modules.push_back(std::move(redpanda_module));
    modules.push_back(std::move(wasi1_module));

    engine->initialize(
      std::move(vm_ctx), std::move(store_ctx), std::move(modules));

    co_return std::move(engine);
}

ss::future<std::unique_ptr<engine>>
make_wasm_engine(std::string_view wasm_module_name, std::string_view wasm_source) {
    co_return co_await wasmedge_wasm_engine::create(wasm_module_name, wasm_source);
}

} // namespace wasm::wasmedge
