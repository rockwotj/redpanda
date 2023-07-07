/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "wasm/wasmtime.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "units.h"
#include "utils/mutex.h"
#include "utils/pointer.h"
#include "vlog.h"
#include "wasm/api.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/rp_module.h"
#include "wasm/schema_registry.h"
#include "wasm/wasi.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <wasmedge/enum_types.h>
#include <wasmtime/config.h>
#include <wasmtime/error.h>
#include <wasmtime/module.h>
#include <wasmtime/store.h>

#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::wasmtime {

namespace {

void check_error(const wasmtime_error_t* error) {
    if (error) {
        wasm_name_t msg;
        wasmtime_error_message(error, &msg);
        std::string str(msg.data, msg.size);
        wasm_byte_vec_delete(&msg);
        vlog(wasm_log.warn, "ERR: {}", str);
        throw wasm_exception(std::move(str), errc::load_failure);
    }
}
void check_trap(const wasm_trap_t* trap) {
    if (trap) {
        wasm_name_t msg{.size = 0, .data = nullptr};
        wasm_trap_message(trap, &msg);
        std::stringstream sb;
        sb << std::string_view(msg.data, msg.size);
        wasm_byte_vec_delete(&msg);
        wasm_frame_vec_t trace{.size = 0, .data = nullptr};
        wasm_trap_trace(trap, &trace);
        for (size_t i = 0; i < trace.size; ++i) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            auto* frame = trace.data[i];
            auto* module_name = wasmtime_frame_module_name(frame);
            auto* function_name = wasmtime_frame_func_name(frame);
            if (module_name && function_name) {
                sb << std::endl
                   << std::string_view(module_name->data, module_name->size)
                   << "::"
                   << std::string_view(
                        function_name->data, function_name->size);
            } else if (module_name) {
                sb << std::endl
                   << std::string_view(module_name->data, module_name->size)
                   << "::??";
            } else if (function_name) {
                sb << std::endl
                   << std::string_view(
                        function_name->data, function_name->size);
            }
        }
        wasm_frame_vec_delete(&trace);
        vlog(wasm_log.warn, "TRAP: {}", sb.str());
        throw wasm_exception(sb.str(), errc::user_code_failure);
    }
}

std::vector<uint64_t> to_raw_values(std::span<const wasmtime_val_t> values) {
    std::vector<uint64_t> raw;
    raw.reserve(values.size());
    for (const wasmtime_val_t val : values) {
        switch (val.kind) {
        case WASMTIME_I32:
            raw.push_back(val.of.i32);
            break;
        case WASMTIME_I64:
            raw.push_back(val.of.i64);
            break;
        case WASMTIME_F32:
            raw.push_back(std::bit_cast<uint32_t>(val.of.f32));
            break;
        case WASMTIME_F64:
            raw.push_back(std::bit_cast<uint64_t>(val.of.f64));
            break;
        default:
            throw wasm_exception(
              ss::format("Unsupported value type: {}", val.kind),
              errc::user_code_failure);
        }
    }
    return raw;
}

wasm_valtype_vec_t
convert_to_wasmtime(const std::vector<ffi::val_type>& ffi_types) {
    wasm_valtype_vec_t wasm_types;
    wasm_valtype_vec_new_uninitialized(&wasm_types, ffi_types.size());
    for (size_t i = 0; i < ffi_types.size(); ++i) {
        switch (ffi_types[i]) {
        case ffi::val_type::i32:
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            wasm_types.data[i] = wasm_valtype_new(WASM_I32);
            break;
        case ffi::val_type::i64:
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            wasm_types.data[i] = wasm_valtype_new(WASM_I64);
            break;
        case ffi::val_type::f32:
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            wasm_types.data[i] = wasm_valtype_new(WASM_F32);
            break;
        case ffi::val_type::f64:
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            wasm_types.data[i] = wasm_valtype_new(WASM_F64);
            break;
        }
    }
    return wasm_types;
}
template<typename T>
wasmtime_val_t convert_to_wasmtime(T value) {
    if constexpr (ss::is_future<T>::value) {
        return convert_to_wasmtime(value.get());
    } else if constexpr (reflection::is_rp_named_type<T>) {
        return convert_to_wasmtime(value());
    } else if constexpr (
      std::is_integral_v<T> && sizeof(T) == sizeof(int64_t)) {
        return wasmtime_val_t{
          .kind = WASMTIME_I64, .of = {.i64 = static_cast<int64_t>(value)}};
    } else if constexpr (std::is_integral_v<T>) {
        return wasmtime_val_t{
          .kind = WASMTIME_I32, .of = {.i32 = static_cast<int32_t>(value)}};
    } else {
        static_assert(
          ffi::detail::dependent_false<T>::value,
          "Unsupported wasm result type");
    }
}

class memory : public ffi::memory {
public:
    explicit memory(wasmtime_context_t* ctx, wasmtime_memory_t* mem)
      : ffi::memory()
      , _ctx(ctx)
      , _underlying(mem) {}

    void* translate_raw(size_t guest_ptr, size_t len) final {
        auto memory_size = wasmtime_memory_data_size(_ctx, _underlying);
        if ((guest_ptr + len) > memory_size) [[unlikely]] {
            throw wasm_exception(
              ss::format(
                "Out of bounds memory access in FFI: {} + {} >= {}",
                guest_ptr,
                len,
                memory_size),
              errc::user_code_failure);
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return wasmtime_memory_data(_ctx, _underlying) + guest_ptr;
    }

private:
    wasmtime_context_t* _ctx;
    wasmtime_memory_t* _underlying;
};

template<auto value>
struct host_function;
template<
  typename Module,
  typename ReturnType,
  typename... ArgTypes,
  ReturnType (Module::*module_func)(ArgTypes...)>
struct host_function<module_func> {
    static void reg(
      wasmtime_linker_t* linker,
      Module* host_module,
      std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        auto inputs = convert_to_wasmtime(ffi_inputs);
        auto outputs = convert_to_wasmtime(ffi_outputs);
        // Takes ownership of inputs and outputs
        handle<wasm_functype_t, wasm_functype_delete> functype{
          wasm_functype_new(&inputs, &outputs)};
        auto* err = wasmtime_linker_define_func(
          linker,
          Module::name.data(),
          Module::name.size(),
          function_name.data(),
          function_name.size(),
          functype.get(),
          [](
            void* env,
            wasmtime_caller_t* caller,
            const wasmtime_val_t* args,
            size_t nargs,
            wasmtime_val_t* results,
            size_t) -> wasm_trap_t* {
              auto host_module = static_cast<Module*>(env);
              constexpr std::string_view memory_export_name = "memory";
              wasmtime_extern_t extern_item;
              bool ok = wasmtime_caller_export_get(
                caller,
                memory_export_name.data(),
                memory_export_name.size(),
                &extern_item);
              if (!ok || extern_item.kind != WASMTIME_EXTERN_MEMORY)
                [[unlikely]] {
                  constexpr std::string_view error = "Missing memory export";
                  vlog(wasm_log.warn, "{}", error);
                  return wasmtime_trap_new(error.data(), error.size());
              }
              memory mem(
                wasmtime_caller_context(caller), &extern_item.of.memory);

              try {
                  auto raw = to_raw_values({args, nargs});

                  auto host_params = ffi::extract_parameters<ArgTypes...>(
                    &mem, raw, 0);
                  if constexpr (std::is_void_v<ReturnType>) {
                      std::apply(
                        module_func,
                        std::tuple_cat(
                          std::make_tuple(host_module), host_params));
                  } else {
                      ReturnType host_result = std::apply(
                        module_func,
                        std::tuple_cat(
                          std::make_tuple(host_module), host_params));
                      *results = convert_to_wasmtime<ReturnType>(
                        std::move(host_result));
                  }
              } catch (const std::exception& e) {
                  vlog(wasm_log.warn, "Failure executing host function: {}", e);
                  std::string_view msg = e.what();
                  return wasmtime_trap_new(msg.data(), msg.size());
              }
              return nullptr;
          },
          host_module,
          nullptr);

        if (err) [[unlikely]] {
            throw wasm::wasm_exception(
              ss::format("Unable to register {}", function_name),
              errc::engine_creation_failure);
        }
    }
};

void register_wasi_module(
  wasi::preview1_module* mod, wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(linker, mod, #name)
    REG_HOST_FN(args_get);
    REG_HOST_FN(args_sizes_get);
    REG_HOST_FN(environ_get);
    REG_HOST_FN(environ_sizes_get);
    REG_HOST_FN(clock_res_get);
    REG_HOST_FN(clock_time_get);
    REG_HOST_FN(fd_advise);
    REG_HOST_FN(fd_allocate);
    REG_HOST_FN(fd_close);
    REG_HOST_FN(fd_datasync);
    REG_HOST_FN(fd_fdstat_get);
    REG_HOST_FN(fd_fdstat_set_flags);
    REG_HOST_FN(fd_filestat_get);
    REG_HOST_FN(fd_filestat_set_size);
    REG_HOST_FN(fd_filestat_set_times);
    REG_HOST_FN(fd_pread);
    REG_HOST_FN(fd_prestat_get);
    REG_HOST_FN(fd_prestat_dir_name);
    REG_HOST_FN(fd_pwrite);
    REG_HOST_FN(fd_read);
    REG_HOST_FN(fd_readdir);
    REG_HOST_FN(fd_renumber);
    REG_HOST_FN(fd_seek);
    REG_HOST_FN(fd_sync);
    REG_HOST_FN(fd_tell);
    REG_HOST_FN(fd_write);
    REG_HOST_FN(path_create_directory);
    REG_HOST_FN(path_filestat_get);
    REG_HOST_FN(path_filestat_set_times);
    REG_HOST_FN(path_link);
    REG_HOST_FN(path_open);
    REG_HOST_FN(path_readlink);
    REG_HOST_FN(path_remove_directory);
    REG_HOST_FN(path_rename);
    REG_HOST_FN(path_symlink);
    REG_HOST_FN(path_unlink_file);
    REG_HOST_FN(poll_oneoff);
    REG_HOST_FN(proc_exit);
    REG_HOST_FN(sched_yield);
    REG_HOST_FN(random_get);
    REG_HOST_FN(sock_accept);
    REG_HOST_FN(sock_recv);
    REG_HOST_FN(sock_send);
    REG_HOST_FN(sock_shutdown);
#undef REG_HOST_FN
}

void register_rp_module(redpanda_module* mod, wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&redpanda_module::name>::reg(linker, mod, #name)
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_record);
    REG_HOST_FN(write_record);
    REG_HOST_FN(get_schema_definition);
    REG_HOST_FN(get_schema_definition_len);
    REG_HOST_FN(create_subject_schema);
#undef REG_HOST_FN
}

class wasmtime_engine : public engine {
public:
    wasmtime_engine(
      ss::sstring user_module_name,
      handle<wasmtime_linker_t, wasmtime_linker_delete> l,
      handle<wasmtime_store_t, wasmtime_store_delete> s,
      std::shared_ptr<wasmtime_module_t> user_module,
      std::unique_ptr<redpanda_module> rp_module,
      std::unique_ptr<wasi::preview1_module> wasi_module,
      wasmtime_instance_t instance)
      : _user_module_name(std::move(user_module_name))
      , _store(std::move(s))
      , _user_module(std::move(user_module))
      , _rp_module(std::move(rp_module))
      , _wasi_module(std::move(wasi_module))
      , _linker(std::move(l))
      , _instance(instance) {}
    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;

    ~wasmtime_engine() override = default;

    std::string_view function_name() const final { return _user_module_name; }

    ss::future<> start() final { return initialize_wasi(); }

    ss::future<> stop() final { return ss::now(); }

    ss::future<model::record_batch>
    transform(const model::record_batch* batch, probe* probe) override {
        vlog(
          wasm_log.info,
          "Transforming batch: {}",
          batch->header().record_count);
        if (batch->compressed()) {
            model::record_batch decompressed
              = co_await storage::internal::decompress_batch(*batch);
            if (decompressed.record_count() == 0) {
                co_return std::move(decompressed);
            }
            co_return co_await invoke_transform(&decompressed, probe);
        } else {
            if (batch->record_count() == 0) {
                co_return batch->copy();
            }
            co_return co_await invoke_transform(batch, probe);
        }
    }

private:
    ss::future<> initialize_wasi() {
        auto* ctx = wasmtime_store_context(_store.get());
        wasmtime_extern_t start;
        bool ok = wasmtime_instance_export_get(
          ctx,
          &_instance,
          wasi::preview_1_start_function_name.data(),
          wasi::preview_1_start_function_name.size(),
          &start);
        if (!ok || start.kind != WASMTIME_EXTERN_FUNC) {
            throw wasm_exception(
              "Missing wasi initialization function", errc::user_code_failure);
        }
        vlog(wasm_log.info, "Initializing wasm function {}", _user_module_name);
        wasm_trap_t* trap_ptr = nullptr;
        handle<wasmtime_error_t, wasmtime_error_delete> error{
          wasmtime_func_call(
            ctx, &start.of.func, nullptr, 0, nullptr, 0, &trap_ptr)};
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        check_error(error.get());
        check_trap(trap.get());
        vlog(wasm_log.info, "Wasm function {} initialized", _user_module_name);
        co_return;
    }

    ss::future<model::record_batch>
    invoke_transform(const model::record_batch* batch, probe* probe) {
        // TODO: There is a high overhead to starting a thread, this should be
        // like a background worker instead.
        return ss::async([this, batch, probe] {
            auto* ctx = wasmtime_store_context(_store.get());
            wasmtime_extern_t cb;
            bool ok = wasmtime_instance_export_get(
              ctx,
              &_instance,
              redpanda_on_record_callback_function_name.data(),
              redpanda_on_record_callback_function_name.size(),
              &cb);
            if (!ok || cb.kind != WASMTIME_EXTERN_FUNC) {
                throw wasm_exception(
                  "Missing wasi initialization function",
                  errc::user_code_failure);
            }
            return _rp_module->for_each_record(
              batch, [this, &ctx, &cb, probe](wasm_call_params params) {
                  _wasi_module->set_timestamp(params.current_record_timestamp);
                  auto ml = probe->latency_measurement();
                  auto mc = probe->cpu_time_measurement();
                  wasmtime_val_t result = {
                    .kind = WASMTIME_I32, .of = {.i32 = -1}};
                  try {
                      std::array<wasmtime_val_t, 4> args = {
                        wasmtime_val_t{
                          .kind = WASMTIME_I32,
                          .of = {.i32 = params.batch_handle}},
                        {.kind = WASMTIME_I32,
                         .of = {.i32 = params.record_handle}},
                        {.kind = WASMTIME_I32,
                         .of = {.i32 = params.record_size}},
                        {.kind = WASMTIME_I32,
                         .of = {.i32 = params.current_record_offset}}};
                      wasm_trap_t* trap_ptr = nullptr;
                      handle<wasmtime_error_t, wasmtime_error_delete> error{
                        wasmtime_func_call(
                          ctx,
                          &cb.of.func,
                          args.data(),
                          args.size(),
                          &result,
                          /*results_size=*/1,
                          &trap_ptr)};
                      handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
                      check_error(error.get());
                      check_trap(trap.get());
                  } catch (...) {
                      probe->transform_error();
                      throw wasm_exception(
                        ss::format(
                          "transform execution {} failed: {}",
                          _user_module_name,
                          std::current_exception()),
                        errc::user_code_failure);
                  }
                  probe->transform_complete();
                  if (result.kind != WASMTIME_I32 || result.of.i32 != 0) {
                      probe->transform_error();
                      throw wasm_exception(
                        ss::format(
                          "transform execution {} resulted in error {}",
                          _user_module_name,
                          result.of.i32),
                        errc::user_code_failure);
                  }
              });
        });
    }

    ss::sstring _user_module_name;
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    std::shared_ptr<wasmtime_module_t> _user_module;
    std::unique_ptr<redpanda_module> _rp_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
    handle<wasmtime_linker_t, wasmtime_linker_delete> _linker;
    wasmtime_instance_t _instance;
};

class wasmtime_engine_factory : public factory {
public:
    wasmtime_engine_factory(
      wasm_engine_t* engine,
      cluster::transform_metadata meta,
      std::shared_ptr<wasmtime_module_t> mod,
      schema_registry* sr)
      : _engine(engine)
      , _module(std::move(mod))
      , _meta(std::move(meta))
      , _sr(sr) {}

    ss::future<std::unique_ptr<engine>> make_engine() final {
        handle<wasmtime_store_t, wasmtime_store_delete> store{
          wasmtime_store_new(_engine, nullptr, nullptr)};
        auto* context = wasmtime_store_context(store.get());
        handle<wasmtime_linker_t, wasmtime_linker_delete> linker{
          wasmtime_linker_new(_engine)};

        auto rp_module = std::make_unique<redpanda_module>(_sr);
        register_rp_module(rp_module.get(), linker.get());

        std::vector<ss::sstring> args{_meta.name()};
        absl::flat_hash_map<ss::sstring, ss::sstring> env = _meta.environment;
        env.emplace("REDPANDA_INPUT_TOPIC", _meta.input_topic.tp());
        env.emplace("REDPANDA_OUTPUT_TOPIC", _meta.output_topics.begin()->tp());
        auto wasi_module = std::make_unique<wasi::preview1_module>(args, env);
        register_wasi_module(wasi_module.get(), linker.get());

        wasmtime_instance_t instance;
        wasm_trap_t* trap_ptr = nullptr;
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_linker_instantiate(
            linker.get(), context, _module.get(), &instance, &trap_ptr));
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        check_error(error.get());
        check_trap(trap.get());

        co_return std::make_unique<wasmtime_engine>(
          _meta.name(),
          std::move(linker),
          std::move(store),
          _module,
          std::move(rp_module),
          std::move(wasi_module),
          instance);
    }

private:
    wasm_engine_t* _engine;
    std::shared_ptr<wasmtime_module_t> _module;
    cluster::transform_metadata _meta;
    schema_registry* _sr;
};

class wasmtime_runtime : public runtime {
public:
    wasmtime_runtime(
      handle<wasm_engine_t, &wasm_engine_delete> h,
      ssx::thread_worker* V_NONNULL t,
      std::unique_ptr<schema_registry> sr)
      : _engine(std::move(h))
      , _worker(t)
      , _sr(std::move(sr)) {}

    ss::future<std::unique_ptr<factory>>
    make_factory(cluster::transform_metadata meta, iobuf buf) override {
        auto user_module = co_await _worker->submit([this, &buf]() {
            bytes b = iobuf_to_bytes(buf);
            wasmtime_module_t* user_module_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_module_new(
                _engine.get(), b.data(), b.size(), &user_module_ptr)};
            std::shared_ptr<wasmtime_module_t> user_module{
              user_module_ptr, wasmtime_module_delete};
            check_error(error.get());
            return user_module;
        });
        co_return std::make_unique<wasmtime_engine_factory>(
          _engine.get(), std::move(meta), user_module, _sr.get());
    }

private:
    handle<wasm_engine_t, &wasm_engine_delete> _engine;
    ssx::thread_worker* V_NONNULL _worker;
    std::unique_ptr<schema_registry> _sr;
};
} // namespace

std::unique_ptr<runtime> create_runtime(
  ssx::thread_worker* V_NONNULL t, std::unique_ptr<schema_registry> sr) {
    wasm_config_t* config = wasm_config_new();

    wasmtime_config_cranelift_opt_level_set(config, WASMTIME_OPT_LEVEL_NONE);
    // TODO: Enable fuel consumption
    wasmtime_config_consume_fuel_set(config, false);
    wasmtime_config_wasm_bulk_memory_set(config, true);
    wasmtime_config_parallel_compilation_set(config, false);
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    wasmtime_config_max_wasm_stack_set(config, 8_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);

    handle<wasm_engine_t, &wasm_engine_delete> engine{
      wasm_engine_new_with_config(config)};
    return std::make_unique<wasmtime_runtime>(
      std::move(engine), t, std::move(sr));
}

} // namespace wasm::wasmtime
