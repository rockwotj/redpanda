#include "seastarx.h"
#include "storage/parser_utils.h"
#include "utils/mutex.h"
#include "wasm/ffi.h"
#include "wasm/probe.h"
#include "wasm/rp_module.h"
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

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;

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
        void* ptr = WasmEdge_MemoryInstanceGetPointer(
          _underlying, guest_ptr, len);
        if (ptr == nullptr) [[unlikely]] {
            throw wasm_exception(
              ss::format(
                "Out of bounds memory access in FFI: {} + {} >= {} (pages)",
                guest_ptr,
                len,
                WasmEdge_MemoryInstanceGetPageSize(_underlying)),
              errc::user_code_failure);
        }
        return ptr;
    }

private:
    WasmEdge_MemoryInstanceContext* _underlying;
};

class wasmedge_wasm_engine;

template<class T>
struct dependent_false : std::false_type {};

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

    wasi::preview1_module* wasi_module() const noexcept {
        return _wasi_module.get();
    }

    redpanda_module* rp_module() const noexcept { return _rp_module.get(); }

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
        _rp_module->prep_call(header, record);
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
            _rp_module->post_call_unclean();
            probe->transform_error();
            throw wasm_exception(
              ss::format("transform execution {} failed", _user_module_name),
              errc::user_code_failure);
        }
        auto user_result = WasmEdge_ValueGetI32(returns[0]);
        if (user_result != 0) {
            _rp_module->post_call_unclean();
            probe->transform_error();
            throw wasm_exception(
              ss::format(
                "transform execution {} resulted in error {}",
                _user_module_name,
                user_result),
              errc::user_code_failure);
        }

        return _rp_module->post_call();
    }

    ss::sstring _user_module_name;
    mutex _mutex;
    std::vector<WasmEdgeModule> _modules;
    WasmEdgeStore _store_ctx = {nullptr, [](auto) {}};
    WasmEdgeVM _vm_ctx = {nullptr, [](auto) {}};
    std::unique_ptr<wasi::preview1_module> _wasi_module
      = std::make_unique<wasi::preview1_module>();
    std::unique_ptr<redpanda_module> _rp_module
      = std::make_unique<redpanda_module>();
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

template<auto value>
struct host_function;
template<
  typename Module,
  typename ReturnType,
  typename... ArgTypes,
  ReturnType (Module::*module_func)(ArgTypes...)>
struct host_function<module_func> {
    static errc reg(
      Module* host_module,
      const WasmEdgeModule& mod,
      std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        std::vector<WasmEdge_ValType> inputs = convert_to_wasmedge(ffi_inputs);
        std::vector<WasmEdge_ValType> outputs = convert_to_wasmedge(
          ffi_outputs);
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
                auto engine = static_cast<Module*>(data);
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
                    if constexpr (std::is_void_v<ReturnType>) {
                        std::apply(
                          module_func,
                          std::tuple_cat(std::make_tuple(engine), host_params));
                    } else {
                        ReturnType host_result = std::apply(
                          module_func,
                          std::tuple_cat(std::make_tuple(engine), host_params));
                        pack_result(guest_returns, std::move(host_result));
                    }
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
            static_cast<void*>(host_module),
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

    auto redpanda_module = create_module(redpanda_module::name);

#define REG_HOST_FN(method)                                                    \
    host_function<&redpanda_module::method>::reg(                              \
      engine->rp_module(), redpanda_module, #method)
    REG_HOST_FN(read_key);
    REG_HOST_FN(read_value);
    REG_HOST_FN(create_output_record);
    REG_HOST_FN(write_key);
    REG_HOST_FN(write_value);
    REG_HOST_FN(num_headers);
    REG_HOST_FN(find_header_by_key);
    REG_HOST_FN(get_header_key_length);
    REG_HOST_FN(get_header_key);
    REG_HOST_FN(get_header_value_length);
    REG_HOST_FN(get_header_value);
    REG_HOST_FN(append_header);
#undef REG_HOST_FN

    result = WasmEdge_VMRegisterModuleFromImport(
      vm_ctx.get(), redpanda_module.get());
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

    auto wasi1_module = create_module(wasi::preview1_module::name);

#define REG_HOST_FN(method)                                                    \
    host_function<&wasi::preview1_module::method>::reg(                        \
      engine->wasi_module(), wasi1_module, #method)
    REG_HOST_FN(clock_time_get);
    REG_HOST_FN(args_sizes_get);
    REG_HOST_FN(args_get);
    REG_HOST_FN(environ_get);
    REG_HOST_FN(environ_sizes_get);
    REG_HOST_FN(fd_close);
    REG_HOST_FN(fd_fdstat_get);
    REG_HOST_FN(fd_prestat_get);
    REG_HOST_FN(fd_prestat_dir_name);
    REG_HOST_FN(fd_read);
    REG_HOST_FN(fd_write);
    REG_HOST_FN(fd_seek);
    REG_HOST_FN(path_open);
    REG_HOST_FN(proc_exit);
    REG_HOST_FN(sched_yield);
#undef REG_HOST_FN

    result = WasmEdge_VMRegisterModuleFromImport(
      vm_ctx.get(), wasi1_module.get());
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

ss::future<std::unique_ptr<engine>> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source) {
    co_return co_await wasmedge_wasm_engine::create(
      wasm_module_name, wasm_source);
}

} // namespace wasm::wasmedge
