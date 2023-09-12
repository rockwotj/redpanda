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
#include "ssx/thread_worker.h"
#include "storage/parser_utils.h"
#include "wasm/api.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/schema_registry_module.h"
#include "wasm/transform_module.h"
#include "wasm/wasi.h"

#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>

#include <wasmtime/config.h>
#include <wasmtime/error.h>
#include <wasmtime/extern.h>
#include <wasmtime/instance.h>
#include <wasmtime/memory.h>
#include <wasmtime/store.h>

#include <csignal>
#include <memory>
#include <pthread.h>
#include <span>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::wasmtime {

namespace {

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

struct host_function_environment {
    void* host_module;
    ss::alien::instance* alien;
    ss::shard_id shard_id;
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
      host_function_environment* env,
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
              auto* fenv = static_cast<host_function_environment*>(env);
              auto* host_module = static_cast<Module*>(fenv->host_module);
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
                          std::make_tuple(host_module),
                          std::move(host_params)));
                  } else if constexpr (ss::is_future<ReturnType>::value) {
                      auto fut = ss::alien::submit_to(
                        *fenv->alien,
                        fenv->shard_id,
                        [host_module,
                         host_params = std::move(host_params)]() mutable {
                            return std::apply(
                              module_func,
                              std::tuple_cat(
                                std::make_tuple(host_module),
                                std::move(host_params)));
                        });
                      *results
                        = convert_to_wasmtime<typename ReturnType::value_type>(
                          std::move(fut).get());
                  } else {
                      ReturnType host_result = std::apply(
                        module_func,
                        std::tuple_cat(
                          std::make_tuple(host_module),
                          std::move(host_params)));
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
          env,
          nullptr);

        if (err) [[unlikely]] {
            throw wasm::wasm_exception(
              ss::format("Unable to register {}", function_name),
              errc::engine_creation_failure);
        }
    }
};

std::unique_ptr<host_function_environment> register_wasi_module(
  wasi::preview1_module* mod,
  wasmtime_linker_t* linker,
  ss::alien::instance* alien,
  ss::shard_id shard_id) {
    auto env = std::make_unique<host_function_environment>(
      mod, alien, shard_id);
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(linker, env.get(), #name)
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
    return env;
}

std::unique_ptr<host_function_environment> register_transform_module(
  transform_module* mod,
  wasmtime_linker_t* linker,
  ss::alien::instance* alien,
  ss::shard_id shard_id) {
    auto env = std::make_unique<host_function_environment>(
      mod, alien, shard_id);
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&transform_module::name>::reg(linker, env.get(), #name)
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_record);
    REG_HOST_FN(write_record);
#undef REG_HOST_FN
    return env;
}
std::unique_ptr<host_function_environment> register_sr_module(
  schema_registry_module* mod,
  wasmtime_linker_t* linker,
  ss::alien::instance* alien,
  ss::shard_id shard_id) {
    auto env = std::make_unique<host_function_environment>(
      mod, alien, shard_id);
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&schema_registry_module::name>::reg(linker, env.get(), #name)
    REG_HOST_FN(get_schema_definition);
    REG_HOST_FN(get_schema_definition_len);
    REG_HOST_FN(get_subject_schema);
    REG_HOST_FN(get_subject_schema_len);
    REG_HOST_FN(create_subject_schema);
#undef REG_HOST_FN
    return env;
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
      std::vector<std::unique_ptr<host_function_environment>>
        host_function_environments,
      wasmtime_instance_t instance,
      ssx::sharded_thread_worker* workers)
      : _user_module_name(std::move(user_module_name))
      , _store(std::move(s))
      , _user_module(std::move(user_module))
      , _transform_module(std::move(transform_module))
      , _sr_module(std::move(sr_module))
      , _wasi_module(std::move(wasi_module))
      , _linker(std::move(l))
      , _instance(instance)
      , _workers(workers)
      , _host_function_environments(std::move(host_function_environments)) {}
    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;

    ~wasmtime_engine() override = default;

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

    ss::future<> start() final { co_return; }

    ss::future<> initialize() final { return initialize_wasi(); }

    ss::future<> stop() final { co_return; }

    struct transform_result {
        model::record_batch batch;
        // it's not safe to access seastar state in an alien thread, so make
        // sure we're passing back the stats we need to record.
        std::vector<uint64_t> latencies;
    };

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        vlog(wasm_log.trace, "Transforming batch: {}", batch.header());
        if (batch.record_count() == 0) {
            co_return std::move(batch);
        }
        if (batch.compressed()) {
            batch = co_await storage::internal::decompress_batch(
              std::move(batch));
        }
        ss::future<transform_result> fut = co_await ss::coroutine::as_future(
          invoke_transform(&batch));
        if (fut.failed()) {
            probe->transform_error();
            std::rethrow_exception(fut.get_exception());
        }
        transform_result r = std::move(fut).get();
        for (uint64_t latency : r.latencies) {
            probe->record_latency(latency);
        }
        co_return std::move(r.batch);
    }

private:
    void reset_fuel() {
        auto* ctx = wasmtime_store_context(_store.get());
        uint64_t fuel = 0;
        wasmtime_context_fuel_remaining(ctx, &fuel);
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_context_add_fuel(ctx, wasmtime_fuel_amount - fuel));
    }

    ss::future<> initialize_wasi() {
        return _workers->submit([this] {
            // TODO: Consider having a different amount of fuel for
            // initialization.
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
                  "Missing wasi initialization function",
                  errc::user_code_failure);
            }
            vlog(
              wasm_log.info,
              "Initializing wasm function {}",
              _user_module_name);
            wasm_trap_t* trap_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_func_call(
                ctx, &start.of.func, nullptr, 0, nullptr, 0, &trap_ptr)};
            handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
            check_error(error.get());
            check_trap(trap.get());
            vlog(
              wasm_log.info, "Wasm function {} initialized", _user_module_name);
        });
    }

    ss::future<transform_result>
    invoke_transform(const model::record_batch* batch) {
        return _workers->submit([this, batch] {
            std::vector<uint64_t> latencies;
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
            auto transformed = _transform_module->for_each_record(
              batch, [this, &ctx, &cb, &latencies](wasm_call_params params) {
                  reset_fuel();
                  _wasi_module->set_timestamp(params.current_record_timestamp);
                  auto recorder = ss::defer(
                    [&latencies,
                     start_time = transform_probe::hist_t::clock_type::now()] {
                        latencies.push_back(
                          std::chrono::duration_cast<
                            transform_probe::hist_t::duration_type>(
                            transform_probe::hist_t::clock_type::now()
                            - start_time)
                            .count());
                    });
                  wasmtime_val_t result = {
                    .kind = WASMTIME_I32, .of = {.i32 = -1}};
                  std::array<wasmtime_val_t, 4> args = {
                    wasmtime_val_t{
                      .kind = WASMTIME_I32, .of = {.i32 = params.batch_handle}},
                    {.kind = WASMTIME_I32, .of = {.i32 = params.record_handle}},
                    {.kind = WASMTIME_I32, .of = {.i32 = params.record_size}},
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
                  if (result.kind != WASMTIME_I32 || result.of.i32 != 0) {
                      throw wasm_exception(
                        ss::format(
                          "transform execution {} resulted in error {}",
                          _user_module_name,
                          result.of.i32),
                        errc::user_code_failure);
                  }
              });
            return transform_result{
              .batch = std::move(transformed),
              .latencies = std::move(latencies),
            };
        });
    }

    ss::sstring _user_module_name;
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    std::shared_ptr<wasmtime_module_t> _user_module;
    std::unique_ptr<transform_module> _transform_module;
    std::unique_ptr<schema_registry_module> _sr_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
    handle<wasmtime_linker_t, wasmtime_linker_delete> _linker;
    wasmtime_instance_t _instance;
    ssx::sharded_thread_worker* _workers;
    std::vector<std::unique_ptr<host_function_environment>>
      _host_function_environments;
};

class wasmtime_engine_factory : public factory {
public:
    wasmtime_engine_factory(
      wasm_engine_t* engine,
      model::transform_metadata meta,
      std::shared_ptr<wasmtime_module_t> mod,
      schema_registry* sr,
      ss::logger* l,
      ssx::sharded_thread_worker* w)
      : _engine(engine)
      , _module(std::move(mod))
      , _meta(std::move(meta))
      , _sr(sr)
      , _logger(l)
      , _workers(w) {}

    ss::future<std::unique_ptr<engine>> make_engine() final {
        ss::alien::instance* alien = &ss::engine().alien();
        ss::shard_id shard_id = ss::this_shard_id();
        return _workers->submit(
          [this, alien, shard_id]() -> std::unique_ptr<engine> {
              handle<wasmtime_store_t, wasmtime_store_delete> store{
                wasmtime_store_new(_engine, nullptr, nullptr)};
              auto* context = wasmtime_store_context(store.get());
              handle<wasmtime_error_t, wasmtime_error_delete> error(
                wasmtime_context_add_fuel(context, 0));
              check_error(error.get());
              handle<wasmtime_linker_t, wasmtime_linker_delete> linker{
                wasmtime_linker_new(_engine)};

              std::vector<std::unique_ptr<host_function_environment>>
                host_function_envs;

              auto xform_module = std::make_unique<transform_module>();
              host_function_envs.push_back(register_transform_module(
                xform_module.get(), linker.get(), alien, shard_id));

              auto sr_module = std::make_unique<schema_registry_module>(_sr);
              host_function_envs.push_back(register_sr_module(
                sr_module.get(), linker.get(), alien, shard_id));

              std::vector<ss::sstring> args{_meta.name()};
              absl::flat_hash_map<ss::sstring, ss::sstring> env
                = _meta.environment;
              env.emplace("REDPANDA_INPUT_TOPIC", _meta.input_topic.tp());
              env.emplace(
                "REDPANDA_OUTPUT_TOPIC", _meta.output_topics.begin()->tp());
              auto wasi_module = std::make_unique<wasi::preview1_module>(
                std::move(args), std::move(env), _logger);
              host_function_envs.push_back(register_wasi_module(
                wasi_module.get(), linker.get(), alien, shard_id));

              wasmtime_instance_t instance;
              wasm_trap_t* trap_ptr = nullptr;
              error.reset(wasmtime_linker_instantiate(
                linker.get(), context, _module.get(), &instance, &trap_ptr));
              handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
              check_error(error.get());
              check_trap(trap.get());

              return std::make_unique<wasmtime_engine>(
                _meta.name(),
                std::move(linker),
                std::move(store),
                _module,
                std::move(xform_module),
                std::move(sr_module),
                std::move(wasi_module),
                std::move(host_function_envs),
                instance,
                _workers);
          });
    }

private:
    wasm_engine_t* _engine;
    std::shared_ptr<wasmtime_module_t> _module;
    model::transform_metadata _meta;
    schema_registry* _sr;
    ss::logger* _logger;
    ssx::sharded_thread_worker* _workers;
};

class wasmtime_runtime : public runtime {
public:
    wasmtime_runtime(
      handle<wasm_engine_t, &wasm_engine_delete> h,
      std::unique_ptr<schema_registry> sr)
      : _engine(std::move(h))
      , _sr(std::move(sr)) {}

    ss::future<> start() override {
        co_await _workers.start({.name = "wasm"});
        co_await ss::smp::invoke_on_all([this] {
            return _workers.submit([] {
                // wasmtime needs some signals for it's handling, make sure we
                // unblock them
                auto mask = ss::make_empty_sigset_mask();
                sigaddset(&mask, SIGSEGV);
                sigaddset(&mask, SIGILL);
                sigaddset(&mask, SIGFPE);
                ss::throw_pthread_error(
                  ::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
            });
        });
    }

    ss::future<> stop() override { return _workers.stop(); }

    ss::future<std::unique_ptr<factory>> make_factory(
      model::transform_metadata meta, iobuf buf, ss::logger* logger) override {
        auto user_module = co_await _workers.submit([this, &meta, &buf] {
            vlog(wasm_log.debug, "compiling wasm module {}", meta.name);
            bytes b = iobuf_to_bytes(buf);
            wasmtime_module_t* user_module_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_module_new(
                _engine.get(), b.data(), b.size(), &user_module_ptr)};
            check_error(error.get());
            std::shared_ptr<wasmtime_module_t> user_module{
              user_module_ptr, wasmtime_module_delete};
            wasm_log.info("Finished compiling wasm module {}", meta.name);
            return user_module;
        });
        co_return std::make_unique<wasmtime_engine_factory>(
          _engine.get(),
          std::move(meta),
          user_module,
          _sr.get(),
          logger,
          &_workers);
    }

private:
    handle<wasm_engine_t, &wasm_engine_delete> _engine;
    std::unique_ptr<schema_registry> _sr;
    ssx::sharded_thread_worker _workers;
};

} // namespace

std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema_registry> sr) {
    wasm_config_t* config = wasm_config_new();

    wasmtime_config_cranelift_opt_level_set(config, WASMTIME_OPT_LEVEL_SPEED);
    wasmtime_config_consume_fuel_set(config, true);
    wasmtime_config_wasm_bulk_memory_set(config, true);
    wasmtime_config_parallel_compilation_set(config, false);
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    wasmtime_config_max_wasm_stack_set(config, 128_KiB);
    // This disables static memory
    wasmtime_config_static_memory_maximum_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_reserved_for_growth_set(config, 0_KiB);
    // don't modify the unwind info as registering these symbols causes C++
    // exceptions to grab a lock in libgcc and deadlock the Redpanda process.
    wasmtime_config_native_unwind_info_set(config, false);

    handle<wasm_engine_t, &wasm_engine_delete> engine{
      wasm_engine_new_with_config(config)};
    return std::make_unique<wasmtime_runtime>(std::move(engine), std::move(sr));
}
} // namespace wasm::wasmtime
