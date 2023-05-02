#include "wasm.h"

#include "errc.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/timestamp.h"
#include "outcome.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>

#include <wasmedge/enum_types.h>
#include <wasmedge/wasmedge.h>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>

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
using input_record_handle = uint32_t;
using output_record_handle = uint32_t;

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;
constexpr size_t max_output_records = 256;

constexpr std::string_view redpanda_module_name = "redpanda";
constexpr std::string_view wasi_preview_1_module_name
  = "wasi_snapshot_preview1";
constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";

WasmEdgeModule create_module(std::string_view name) {
    auto wrapped = WasmEdge_StringWrap(name.data(), name.size());
    return {
      WasmEdge_ModuleInstanceCreate(wrapped), &WasmEdge_ModuleInstanceDelete};
}

struct record_builder {
    iobuf key;
    iobuf value;
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

class wasmedge_wasm_engine {
public:
    static result<std::unique_ptr<wasmedge_wasm_engine>, errc>
    Create(std::vector<uint8_t> module_source) {
        auto config_ctx = WasmEdgeConfig(
          WasmEdge_ConfigureCreate(), &WasmEdge_ConfigureDelete);

        auto store_ctx = WasmEdgeStore(
          WasmEdge_StoreCreate(), &WasmEdge_StoreDelete);

        auto vm_ctx = WasmEdgeVM(
          WasmEdge_VMCreate(config_ctx.get(), store_ctx.get()),
          &WasmEdge_VMDelete);

        auto engine = std::unique_ptr<wasmedge_wasm_engine>(
          new wasmedge_wasm_engine());

        WasmEdge_Result result;

        auto redpanda_module = create_module(redpanda_module_name);

        engine->register_host_function<&wasmedge_wasm_engine::read_key>(
          redpanda_module.get(), "read_key");
        engine->register_host_function<&wasmedge_wasm_engine::read_value>(
          redpanda_module.get(), "read_value");

        WasmEdge_VMRegisterModuleFromImport(
          vm_ctx.get(), redpanda_module.get());

        auto wasi1_module = create_module(wasi_preview_1_module_name);

        // TODO: Register wasi modules

        WasmEdge_VMRegisterModuleFromImport(vm_ctx.get(), wasi1_module.get());

        auto loader_ctx = WasmEdgeLoader(
          WasmEdge_LoaderCreate(config_ctx.get()), &WasmEdge_LoaderDelete);

        WasmEdge_ASTModuleContext* module_ctx_ptr = nullptr;
        result = WasmEdge_LoaderParseFromBuffer(
          loader_ctx.get(),
          &module_ctx_ptr,
          module_source.data(),
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

        result = WasmEdge_VMLoadWasmFromASTModule(
          vm_ctx.get(), module_ctx.get());

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

    // TODO: How akward is this API? Can we flatten the batches.
    ss::future<std::vector<model::record_batch>>
    transform(model::record_batch&& batch) {
        model::record_batch decompressed
          = co_await storage::internal::decompress_batch(std::move(batch));

        std::vector<model::record_batch> batches;

        co_await ss::async(
          [this, &batches](model::record_batch decompressed) {
              decompressed.for_each_record(
                [this, &batches, &decompressed](model::record record) {
                    auto output_batch = invoke_transform(
                      decompressed.header(), std::move(record));
                    if (output_batch.has_value()) {
                        batches.push_back(std::move(output_batch.value()));
                    }
                });
          },
          std::move(decompressed));

        co_return batches;
    }

private:
    wasmedge_wasm_engine()
      : _store_ctx(nullptr, [](auto) {})
      , _vm_ctx(nullptr, [](auto) {}){};

    void initialize(
      WasmEdgeVM vm_ctx,
      WasmEdgeStore store_ctx,
      std::vector<WasmEdgeModule> modules) {
        _vm_ctx = std::move(vm_ctx);
        _store_ctx = std::move(store_ctx);
        _modules = std::move(modules);
    }

    std::optional<model::record_batch> invoke_transform(
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
        // TODO: Put in a scheduling group
        std::array<WasmEdge_Value, 1> params = {
          WasmEdge_ValueGenI32(fixed_input_record_handle)};
        std::array<WasmEdge_Value, 1> returns = {WasmEdge_ValueGenI32(0)};
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
        if (WasmEdge_ValueGetI32(returns[0]) != 0) {
            _call_ctx = std::nullopt;
            throw wasm_exception(ss::format(
              "transform execution {} resulted in an error",
              user_transform_name));
        }
        storage::record_batch_builder builder(
          header.type,
          model::offset(header.base_offset() + record.offset_delta()));
        builder.set_producer_identity(
          header.producer_id, header.producer_epoch);
        builder.set_timestamp(model::timestamp(
          header.first_timestamp() + record.timestamp_delta()));
        for (auto& output_record : _call_ctx->output_records) {
            builder.add_raw_kw(
              std::move(output_record.key),
              std::move(output_record.value),
              std::move(output_record.headers));
        }
        _call_ctx = std::nullopt;
        if (builder.empty()) {
            return std::nullopt;
        }
        return std::move(builder).build();
    }

    std::optional<transform_context> _call_ctx;

    // Start ABI exports
    // This is a small set just to get the ball rolling

    read_result
    read_key(input_record_handle handle, uint8_t* data, uint32_t len) {
        if (handle != fixed_input_record_handle || !_call_ctx) {
            return -1;
        }
        size_t remaining = _call_ctx->key.segment_bytes_left();
        size_t amount = std::min(size_t(len), remaining);
        _call_ctx->key.consume_to(amount, data);
        return int32_t(amount);
    }

    read_result
    read_value(input_record_handle handle, uint8_t* data, uint32_t len) {
        if (handle != fixed_input_record_handle || !_call_ctx) {
            return -1;
        }
        size_t remaining = _call_ctx->value.segment_bytes_left();
        size_t amount = std::min(size_t(len), remaining);
        _call_ctx->value.consume_to(amount, data);
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
        return idx;
    }

    write_result
    write_key(output_record_handle handle, uint8_t* data, uint32_t len) {
        if (
          !_call_ctx || handle < 0
          || handle >= _call_ctx->output_records.size()) {
            return -1;
        }
        // TODO: Define a limit here?
        _call_ctx->output_records[handle].key.append(data, len);
        return int32_t(len);
    }

    write_result
    write_value(output_record_handle handle, uint8_t* data, uint32_t len) {
        if (
          !_call_ctx || handle < 0
          || handle >= _call_ctx->output_records.size()) {
            return -1;
        }
        // TODO: Define a limit here?
        _call_ctx->output_records[handle].value.append(data, len);
        return int32_t(len);
    }

    // End ABI exports

    template<int32_t (wasmedge_wasm_engine::*engine_func)(
      uint32_t, uint8_t*, uint32_t)>
    errc register_host_function(
      WasmEdge_ModuleInstanceContext* mod, std::string_view function_name) {
        std::array<WasmEdge_ValType, 3> inputs = {
          WasmEdge_ValType_I32,
          WasmEdge_ValType_I32,
          WasmEdge_ValType_I32,
        };
        std::array<WasmEdge_ValType, 1> outputs = {WasmEdge_ValType_I32};
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

        WasmEdge_FunctionInstanceContext* func
          = WasmEdge_FunctionInstanceCreate(
            func_type_ctx.get(),
            [](
              void* data,
              const WasmEdge_CallingFrameContext* calling_frame_ctx,
              const WasmEdge_Value* params,
              WasmEdge_Value* returns) {
                auto engine = static_cast<wasmedge_wasm_engine*>(data);
                int32_t handle = WasmEdge_ValueGetI32(params[0]);
                auto guest_ptr = static_cast<uint32_t>(
                  WasmEdge_ValueGetI32(params[1]));
                uint32_t guest_ptr_len = WasmEdge_ValueGetI32(params[2]);
                WasmEdge_MemoryInstanceContext* mem_ctx
                  = WasmEdge_CallingFrameGetMemoryInstance(
                    calling_frame_ctx, 0);
                uint8_t* host_ptr = WasmEdge_MemoryInstanceGetPointer(
                  mem_ctx, guest_ptr, guest_ptr_len);
                // TODO: Handle errors
                int32_t result = (engine->*engine_func)(
                  handle, host_ptr, guest_ptr_len);
                returns[0] = WasmEdge_ValueGenI32(result);
                return WasmEdge_Result_Success;
            },
            static_cast<void*>(this),
            /*cost=*/0);

        if (!func) {
            vlog(
              wasm_log.warn,
              "Failed to register host function: {}",
              function_name);
            return errc::load_failure;
        }
        WasmEdge_ModuleInstanceAddFunction(
          mod,
          WasmEdge_StringWrap(function_name.data(), function_name.size()),
          func);

        return errc::success;
    }

    std::vector<WasmEdgeModule> _modules;
    WasmEdgeStore _store_ctx;
    WasmEdgeVM _vm_ctx;
};
} // namespace

} // namespace wasm
