/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "wasm/wasmtime.h"

#include "model/record.h"
#include "model/record_batch_types.h"
#include "ssx/future-util.h"
#include "ssx/thread_worker.h"
#include "storage/parser_utils.h"
#include "utils/mutex.h"
#include "wasm/api.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/schema_registry_module.h"
#include "wasm/transform_module.h"
#include "wasm/wasi.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <wasmtime/config.h>
#include <wasmtime/error.h>
#include <wasmtime/extern.h>
#include <wasmtime/instance.h>
#include <wasmtime/memory.h>
#include <wasmtime/store.h>

#include <csignal>
#include <exception>
#include <memory>
#include <pthread.h>
#include <span>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::wasmtime {

namespace {

class engine_stop_exception final : public std::exception {};

void unblock_signals() {
    // wasmtime needs some signals for it's handling, make sure we
    // unblock them.
    auto mask = ss::make_empty_sigset_mask();
    sigaddset(&mask, SIGSEGV);
    sigaddset(&mask, SIGILL);
    sigaddset(&mask, SIGFPE);
    ss::throw_pthread_error(::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
}

constexpr uint64_t wasmtime_fuel_amount = 1'000'000'000;

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

void check_error(const wasmtime_error_t* error) {
    if (!error) {
        return;
    }
    wasm_name_t msg;
    wasmtime_error_message(error, &msg);
    std::string str(msg.data, msg.size);
    wasm_byte_vec_delete(&msg);
    throw wasm_exception(std::move(str), errc::load_failure);
}
void check_trap(const wasm_trap_t* trap) {
    if (!trap) {
        return;
    }
    wasm_name_t msg{.size = 0, .data = nullptr};
    wasm_trap_message(trap, &msg);
    std::stringstream sb;
    sb << std::string_view(msg.data, msg.size);
    wasm_byte_vec_delete(&msg);
    wasm_frame_vec_t trace{.size = 0, .data = nullptr};
    wasm_trap_trace(trap, &trace);
    for (wasm_frame_t* frame : std::span(trace.data, trace.size)) {
        auto* module_name = wasmtime_frame_module_name(frame);
        auto* function_name = wasmtime_frame_func_name(frame);
        if (module_name && function_name) {
            sb << std::endl
               << std::string_view(module_name->data, module_name->size) << "::"
               << std::string_view(function_name->data, function_name->size);
        } else if (module_name) {
            sb << std::endl
               << std::string_view(module_name->data, module_name->size)
               << "::??";
        } else if (function_name) {
            sb << std::endl
               << std::string_view(function_name->data, function_name->size);
        }
    }
    wasm_frame_vec_delete(&trace);
    throw wasm_exception(sb.str(), errc::user_code_failure);
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
    std::span<wasm_valtype_t*> wasm_types_span{
      wasm_types.data, ffi_types.size()};
    for (size_t i = 0; i < ffi_types.size(); ++i) {
        switch (ffi_types[i]) {
        case ffi::val_type::i32:
            wasm_types_span[i] = wasm_valtype_new(WASM_I32);
            break;
        case ffi::val_type::i64:
            wasm_types_span[i] = wasm_valtype_new(WASM_I64);
            break;
        }
    }
    return wasm_types;
}
template<typename T>
wasmtime_val_t convert_to_wasmtime(T value) {
    if constexpr (reflection::is_rp_named_type<T>) {
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
    explicit memory(wasmtime_context_t* ctx)
      : _ctx(ctx)
      , _underlying() {}

    void* translate_raw(size_t guest_ptr, size_t len) final {
        auto memory_size = wasmtime_memory_data_size(_ctx, &_underlying);
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
        return wasmtime_memory_data(_ctx, &_underlying) + guest_ptr;
    }

    void set_underlying_memory(wasmtime_memory_t* m) { _underlying = *m; }

private:
    wasmtime_context_t* _ctx;
    wasmtime_memory_t _underlying;
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
      wasmtime_linker_t* linker, Module* mod, std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        auto inputs = convert_to_wasmtime(ffi_inputs);
        auto outputs = convert_to_wasmtime(ffi_outputs);
        // Takes ownership of inputs and outputs
        handle<wasm_functype_t, wasm_functype_delete> functype{
          wasm_functype_new(&inputs, &outputs)};

        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_linker_define_func(
            linker,
            Module::name.data(),
            Module::name.size(),
            function_name.data(),
            function_name.size(),
            functype.get(),
            &invoke_host_fn,
            mod,
            /*finalizer=*/nullptr));
        check_error(error.get());
    }

private:
    /**
     * All the boilerplate setup needed to invoke a host function
     * from the VM.
     *
     * Extracts the memory module, and handles exceptions from our
     * host functions.
     */
    static wasm_trap_t* invoke_host_fn(
      void* env,
      wasmtime_caller_t* caller,
      const wasmtime_val_t* args,
      size_t nargs,
      wasmtime_val_t* results,
      size_t nresults) {
        auto* host_module = static_cast<Module*>(env);
        memory mem(wasmtime_caller_context(caller));
        wasm_trap_t* trap = extract_memory(caller, &mem);
        if (trap != nullptr) {
            return trap;
        }
        try {
            do_invoke_host_fn(
              host_module, &mem, {args, nargs}, {results, nresults});
            return nullptr;
        } catch (const engine_stop_exception&) {
            constexpr std::string_view stop_msg = "engine stopped";
            return wasmtime_trap_new(stop_msg.data(), stop_msg.size());
        } catch (const std::exception& e) {
            vlog(wasm_log.warn, "Failure executing host function: {}", e);
            std::string_view msg = e.what();
            return wasmtime_trap_new(msg.data(), msg.size());
        }
    }

    /**
     * Translate wasmtime types into our native types, then invoke
     * the corresponding host function, translating the response
     * types into the correct types.
     */
    static void do_invoke_host_fn(
      Module* host_module,
      memory* mem,
      std::span<const wasmtime_val_t> args,
      std::span<wasmtime_val_t> results) {
        auto raw = to_raw_values(args);
        auto host_params = ffi::extract_parameters<ArgTypes...>(mem, raw, 0);
        if constexpr (std::is_void_v<ReturnType>) {
            std::apply(
              module_func,
              std::tuple_cat(
                std::make_tuple(host_module), std::move(host_params)));
        } else if constexpr (ss::is_future<ReturnType>::value) {
            typename ReturnType::value_type host_result
              = std::apply(
                  module_func,
                  std::tuple_cat(
                    std::make_tuple(host_module), std::move(host_params)))
                  .get();
            results[0] = convert_to_wasmtime<typename ReturnType::value_type>(
              host_result);
        } else {
            ReturnType host_result = std::apply(
              module_func,
              std::tuple_cat(
                std::make_tuple(host_module), std::move(host_params)));
            results[0] = convert_to_wasmtime<ReturnType>(
              std::move(host_result));
        }
    }

    /**
     * Our ABI contract expects a "memory" export from the module that we can
     * use to access the VM's memory space.
     */
    static wasm_trap_t* extract_memory(wasmtime_caller_t* caller, memory* mem) {
        constexpr std::string_view memory_export_name = "memory";
        wasmtime_extern_t extern_item;
        bool ok = wasmtime_caller_export_get(
          caller,
          memory_export_name.data(),
          memory_export_name.size(),
          &extern_item);
        if (!ok || extern_item.kind != WASMTIME_EXTERN_MEMORY) [[unlikely]] {
            constexpr std::string_view error = "Missing memory export";
            vlog(wasm_log.warn, "{}", error);
            return wasmtime_trap_new(error.data(), error.size());
        }
        mem->set_underlying_memory(&extern_item.of.memory);
        return nullptr;
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

void register_transform_module(
  transform_module* mod, wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&transform_module::name>::reg(linker, mod, #name)
    REG_HOST_FN(check_abi_version_1);
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_record);
    REG_HOST_FN(write_record);
#undef REG_HOST_FN
}

void register_sr_module(
  schema_registry_module* mod, wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&schema_registry_module::name>::reg(linker, mod, #name)
    REG_HOST_FN(get_schema_definition);
    REG_HOST_FN(get_schema_definition_len);
    REG_HOST_FN(get_subject_schema);
    REG_HOST_FN(get_subject_schema_len);
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
      std::unique_ptr<transform_module> transform_module,
      std::unique_ptr<schema_registry_module> sr_module,
      std::unique_ptr<wasi::preview1_module> wasi_module,
      wasmtime_instance_t instance)
      : _user_module_name(std::move(user_module_name))
      , _store(std::move(s))
      , _user_module(std::move(user_module))
      , _transform_module(std::move(transform_module))
      , _sr_module(std::move(sr_module))
      , _wasi_module(std::move(wasi_module))
      , _linker(std::move(l))
      , _instance(instance) {}
    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;

    ~wasmtime_engine() override {
        vlog(
          wasm_log.debug,
          "deleting transform module: {}",
          uintptr_t(_transform_module.get()));
    };

    std::string_view function_name() const final { return _user_module_name; }

    uint64_t memory_usage_size_bytes() const final {
        std::string_view memory_export_name = "memory";
        auto* ctx = wasmtime_store_context(_store.get());
        wasmtime_extern_t memory_extern;
        bool ok = wasmtime_instance_export_get(
          ctx,
          &_instance,
          memory_export_name.data(),
          memory_export_name.size(),
          &memory_extern);
        if (!ok || memory_extern.kind != WASMTIME_EXTERN_MEMORY) {
            return 0;
        }
        return wasmtime_memory_data_size(ctx, &memory_extern.of.memory);
    };

    ss::future<> start() final {
        _transform_module->start();
        _mu = {};
        co_return;
    }

    ss::future<> initialize() final {
        auto ready_fut = _transform_module->wait_ready();
        _vm_thread_gate.enter();
        ssx::background
          = ss::async([this] {
                std::exception_ptr ex;
                try {
                    initialize_wasi();
                    ex = std::make_exception_ptr(wasm::wasm_exception(
                      "main exited", errc::engine_not_running));
                } catch (...) {
                    ex = std::current_exception();
                }
                vlog(
                  wasm_log.debug,
                  "finishing up _start function from wasi and "
                  "stopping module, {} "
                  "{} {}",
                  ex,
                  ss::thread::running_in_thread(),
                  uintptr_t(_transform_module.get()));
                _transform_module->stop();
                vlog(
                  wasm_log.debug,
                  "finished up stopping the transform module: {} {}",
                  uintptr_t(_transform_module.get()),
                  ss::thread::running_in_thread());
            }).then([this]() mutable {
                vlog(wasm_log.debug, "main thread finished");
                _vm_thread_gate.leave();
                vlog(wasm_log.debug, "main thread finished");
            });
        co_await std::move(ready_fut);
    }

    ss::future<> stop() final {
        _mu.broken();
        vlog(wasm_log.info, "Stopping wasm engine...");
        _transform_module->abort(
          std::make_exception_ptr(engine_stop_exception()));
        auto fut = _vm_thread_gate.close();
        while (_vm_thread_gate.get_count() > 0) {
            vlog(
              wasm_log.info,
              "Sleepy wasm engine {}",
              _vm_thread_gate.get_count());
            co_await ss::maybe_yield();
        }
        co_await std::move(fut);
        vlog(
          wasm_log.trace,
          "_main_task exitted: {}",
          ss::thread::running_in_thread());
        co_return;
    }

    ss::future<ss::chunked_fifo<model::record_batch>>
    transform(model::record_batch batch, transform_probe* probe) override {
        vlog(wasm_log.trace, "Transforming batch: {}", batch.header());
        if (batch.record_count() == 0) {
            ss::chunked_fifo<model::record_batch> batches;
            co_return std::move(batches);
        }
        if (batch.compressed()) {
            batch = co_await storage::internal::decompress_batch(
              std::move(batch));
        }
        ss::future<ss::chunked_fifo<model::record_batch>> fut
          = co_await ss::coroutine::as_future(invoke_transform(&batch, probe));
        if (fut.failed()) {
            probe->transform_error();
            std::rethrow_exception(fut.get_exception());
        }
        co_return std::move(fut).get();
    }

private:
    void reset_fuel() {
        auto* ctx = wasmtime_store_context(_store.get());
        uint64_t fuel = 0;
        wasmtime_context_fuel_remaining(ctx, &fuel);
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_context_add_fuel(ctx, wasmtime_fuel_amount - fuel));
    }

    void initialize_wasi() {
        // seastar threads swap context, which has it's own
        // signals that it masks
        unblock_signals();
        // TODO: Consider having a different amount of fuel
        // for initialization.
        reset_fuel();
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
              "Missing main function", errc::user_code_failure);
        }
        vlog(wasm_log.info, "Initializing wasm function {}", _user_module_name);
        wasm_trap_t* trap_ptr = nullptr;
        handle<wasmtime_error_t, wasmtime_error_delete> error{
          wasmtime_func_call(
            ctx, &start.of.func, nullptr, 0, nullptr, 0, &trap_ptr)};
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        try {
            check_error(error.get());
            check_trap(trap.get());
        } catch (const engine_stop_exception& ex) {
            // This can happen if `stop` is called, so don't
            // log in this case.
            return;
        } catch (...) {
            auto ex = std::current_exception();
            vlog(
              wasm_log.warn,
              "Wasm function {} error: {}",
              _user_module_name,
              ex);
            std::rethrow_exception(ex);
        }
        vlog(wasm_log.warn, "Wasm function {} finished", _user_module_name);
    }

    ss::future<ss::chunked_fifo<model::record_batch>>
    invoke_transform(const model::record_batch* batch, transform_probe* p) {
        auto u = co_await _mu.get_units();
        std::unique_ptr<transform_probe::hist_t::measurement> measurement;
        auto result = co_await _transform_module->transform(
          {.batch = batch, .pre_record_callback = [this, p, &measurement] {
               reset_fuel();
               measurement = p->latency_measurement();
           }});
        co_return result;
    }

    mutex _mu;
    ss::gate _vm_thread_gate;
    ss::sstring _user_module_name;
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    std::shared_ptr<wasmtime_module_t> _user_module;
    std::unique_ptr<transform_module> _transform_module;
    std::unique_ptr<schema_registry_module> _sr_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
    handle<wasmtime_linker_t, wasmtime_linker_delete> _linker;
    wasmtime_instance_t _instance;
};

class wasmtime_engine_factory : public factory {
public:
    wasmtime_engine_factory(
      wasm_engine_t* engine,
      model::transform_metadata meta,
      std::shared_ptr<wasmtime_module_t> mod,
      schema_registry* sr,
      ss::logger* l,
      ssx::sharded_thread_worker* pool)
      : _engine(engine)
      , _module(std::move(mod))
      , _meta(std::move(meta))
      , _sr(sr)
      , _logger(l)
      , _alien_thread_pool(pool) {}

    ss::future<std::unique_ptr<engine>> make_engine() final {
        vlog(wasm_log.debug, "creating...");
        handle<wasmtime_store_t, wasmtime_store_delete> store{
          wasmtime_store_new(_engine, nullptr, nullptr)};
        auto* context = wasmtime_store_context(store.get());
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_context_add_fuel(context, 0));
        check_error(error.get());
        handle<wasmtime_linker_t, wasmtime_linker_delete> linker{
          wasmtime_linker_new(_engine)};

        std::vector<ss::sstring> args{_meta.name()};
        absl::flat_hash_map<ss::sstring, ss::sstring> env = _meta.environment;
        env.emplace("REDPANDA_INPUT_TOPIC", _meta.input_topic.tp());
        env.emplace("REDPANDA_OUTPUT_TOPIC", _meta.output_topics.begin()->tp());
        // TODO(rockwood): Don't hard code this string, but look it
        // up from configuration.
        env.emplace(
          "REDPANDA_OUTPUT_TOPIC_MAX_BATCH_SIZE", ss::format("{}", 1_MiB));
        auto wasi_module = std::make_unique<wasi::preview1_module>(
          std::move(args), std::move(env), _logger);

        auto xform_module = std::make_unique<transform_module>(
          wasi_module.get());

        auto sr_module = std::make_unique<schema_registry_module>(_sr);
        // Register the modules on an alien thread because cranelift
        // needs largish stacks which aren't available in unit
        // tests.
        wasmtime_instance_t instance = co_await _alien_thread_pool->submit(
          [this,
           context,
           sr = sr_module.get(),
           wasi = wasi_module.get(),
           xform = xform_module.get(),
           l = linker.get()] {
              register_wasi_module(wasi, l);
              register_transform_module(xform, l);
              register_sr_module(sr, l);

              wasmtime_instance_t instance;
              wasm_trap_t* trap_ptr = nullptr;
              handle<wasmtime_error_t, wasmtime_error_delete> error(
                wasmtime_linker_instantiate(
                  l, context, _module.get(), &instance, &trap_ptr));
              handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
              check_error(error.get());
              check_trap(trap.get());
              return instance;
          });

        co_return std::make_unique<wasmtime_engine>(
          _meta.name(),
          std::move(linker),
          std::move(store),
          _module,
          std::move(xform_module),
          std::move(sr_module),
          std::move(wasi_module),
          instance);
    }

private:
    wasm_engine_t* _engine;
    std::shared_ptr<wasmtime_module_t> _module;
    model::transform_metadata _meta;
    schema_registry* _sr;
    ss::logger* _logger;
    ssx::sharded_thread_worker* _alien_thread_pool;
};

class wasmtime_runtime : public runtime {
public:
    wasmtime_runtime(
      handle<wasm_engine_t, &wasm_engine_delete> h,
      std::unique_ptr<schema_registry> sr)
      : _engine(std::move(h))
      , _sr(std::move(sr)) {}

    ss::future<> start() override {
        vlog(wasm_log.debug, "starting runtime...");
        co_await _alien_thread_pool.start({.name = "wasm"});
    }

    ss::future<> stop() override {
        return _alien_thread_pool.stop();
        vlog(wasm_log.debug, "stopped runtime...");
    }

    ss::future<std::unique_ptr<factory>> make_factory(
      model::transform_metadata meta, iobuf buf, ss::logger* logger) override {
        vlog(wasm_log.debug, "compiling...");
        auto user_module = co_await _alien_thread_pool.submit([this,
                                                               &meta,
                                                               &buf] {
            vlog(
              wasm_log.info,
              "Starting compilation for wasm module {}",
              meta.name);
            // This can be a large contiguous allocation, however
            // it happens on an alien thread so it bypasses the
            // seastar allocator.
            bytes b = iobuf_to_bytes(buf);
            wasmtime_module_t* user_module_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_module_new(
                _engine.get(), b.data(), b.size(), &user_module_ptr)};
            check_error(error.get());
            std::shared_ptr<wasmtime_module_t> user_module{
              user_module_ptr, wasmtime_module_delete};
            vlog(wasm_log.info, "Finished compiling wasm module {}", meta.name);
            return user_module;
        });
        co_return std::make_unique<wasmtime_engine_factory>(
          _engine.get(),
          std::move(meta),
          user_module,
          _sr.get(),
          logger,
          &_alien_thread_pool);
    }

private:
    handle<wasm_engine_t, &wasm_engine_delete> _engine;
    std::unique_ptr<schema_registry> _sr;
    ssx::sharded_thread_worker _alien_thread_pool;
};

} // namespace

std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema_registry> sr) {
    wasm_config_t* config = wasm_config_new();

    // Spend more time compiling so that we can have faster code.
    wasmtime_config_cranelift_opt_level_set(config, WASMTIME_OPT_LEVEL_SPEED);
    // Fuel allows us to stop execution after some time.
    wasmtime_config_consume_fuel_set(config, true);
    // We want to enable memcopy and other efficent memcpy operators
    wasmtime_config_wasm_bulk_memory_set(config, true);
    // wasmtime_config_parallel_compilation_set(config, false);
    // Set max stack size to generally be as big as a contiguous memory
    // region we're willing to allocate in Redpanda.
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    wasmtime_config_max_wasm_stack_set(config, 128_KiB);
    // This disables static memory, see:
    // https://docs.wasmtime.dev/contributing-architecture.html#linear-memory
    wasmtime_config_static_memory_maximum_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_reserved_for_growth_set(config, 0_KiB);
    // Don't modify the unwind info as registering these symbols causes
    // C++ exceptions to grab a lock in libgcc and deadlock the Redpanda
    // process.
    wasmtime_config_native_unwind_info_set(config, false);

    handle<wasm_engine_t, &wasm_engine_delete> engine{
      wasm_engine_new_with_config(config)};
    return std::make_unique<wasmtime_runtime>(std::move(engine), std::move(sr));
}
} // namespace wasm::wasmtime
