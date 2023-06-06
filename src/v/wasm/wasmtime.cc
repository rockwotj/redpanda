// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "wasm/wasmtime.h"

#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "units.h"
#include "utils/mutex.h"
#include "vlog.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/probe.h"
#include "wasm/rp_module.h"
#include "wasm/wasi.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <wasmtime/config.h>
#include <wasmtime/error.h>
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

thread_local bool is_executing = false;

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

void check_error(const wasmtime_error_t* error) {
    if (error) {
        wasm_name_t msg;
        wasmtime_error_message(error, &msg);
        std::string str(msg.data, msg.size);
        wasm_byte_vec_delete(&msg);
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
        throw wasm_exception(sb.str(), errc::user_code_failure);
    }
}

static ss::logger wasmtime_log("wasmtime");

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
            wasm_types.data[i] = wasm_valtype_new(WASM_I32);
            break;
        case ffi::val_type::i64:
            wasm_types.data[i] = wasm_valtype_new(WASM_I64);
            break;
        case ffi::val_type::f32:
            wasm_types.data[i] = wasm_valtype_new(WASM_F32);
            break;
        case ffi::val_type::f64:
            wasm_types.data[i] = wasm_valtype_new(WASM_F64);
            break;
        }
    }
    return wasm_types;
}

class memory : public ffi::memory {
public:
    explicit memory(wasmtime_context_t* ctx, wasmtime_memory_t* mem)
      : ffi::memory()
      , _ctx(ctx)
      , _underlying(mem) {}

    void* translate(size_t guest_ptr, size_t len) final {
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
                  vlog(wasmtime_log.warn, "{}", error);
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
                      if constexpr (
                        std::is_integral_v<ReturnType>
                        && sizeof(ReturnType) == 8) {
                          results[0] = wasmtime_val_t{
                            .kind = WASMTIME_I64,
                            .of = {.i64 = static_cast<int64_t>(host_result)}};
                      } else {
                          results[0] = wasmtime_val_t{
                            .kind = WASMTIME_I32,
                            .of = {.i32 = static_cast<int32_t>(host_result)}};
                      }
                  }
              } catch (const std::exception& e) {
                  vlog(
                    wasmtime_log.warn,
                    "Failure executing host function: {}",
                    e);
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
#define REG_HOST_FN(name)                                                      \
    host_function<&redpanda_module::name>::reg(linker, mod, #name)
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
}

class wasmtime_engine : public engine {
public:
    static std::unique_ptr<wasmtime_engine>
    make(std::string_view wasm_module_name, std::string_view wasm_source);

    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;

    ~wasmtime_engine() override = default;

    std::string_view function_name() const final { return _user_module_name; }

    ss::future<> start() final { return initialize_wasi(); }

    ss::future<> stop() final { return ss::now(); }

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

        // TODO: Put in a scheduling group
        co_await model::for_each_record(
          decompressed,
          [this, &transformed_records, &decompressed, probe](
            model::record& record) {
              return invoke_transform(decompressed.header(), record, probe)
                .then([&transformed_records](auto output) {
                    transformed_records.insert(
                      transformed_records.end(),
                      std::make_move_iterator(output.begin()),
                      std::make_move_iterator(output.end()));
                });
          });

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

    wasmtime_engine(
      std::string_view user_module_name,
      wasm_engine_t* e,
      handle<wasmtime_linker_t, wasmtime_linker_delete> l,
      handle<wasmtime_store_t, wasmtime_store_delete> s,
      std::shared_ptr<wasmtime_module_t> user_module,
      std::unique_ptr<redpanda_module> rp_module,
      std::unique_ptr<wasi::preview1_module> wasi_module,
      wasmtime_instance_t instance)
      : engine()
      , _user_module_name(user_module_name)
      , _engine(e)
      , _store(std::move(s))
      , _user_module(std::move(user_module))
      , _rp_module(std::move(rp_module))
      , _wasi_module(std::move(wasi_module))
      , _linker(std::move(l))
      , _instance(instance) {}

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
        vlog(
          wasmtime_log.info,
          "Initializing wasm function {}",
          _user_module_name);
        is_executing = true;
        wasm_trap_t* trap_ptr = nullptr;
        handle<wasmtime_error_t, wasmtime_error_delete> error{
          wasmtime_func_call(
            ctx, &start.of.func, nullptr, 0, nullptr, 0, &trap_ptr)};
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        is_executing = false;
        check_error(error.get());
        check_trap(trap.get());
        vlog(
          wasmtime_log.info, "Wasm function {} initialized", _user_module_name);
        co_return;
    }

    ss::future<std::vector<model::record>> invoke_transform(
      const model::record_batch_header& header,
      model::record& record,
      probe* probe) {
        _rp_module->prep_call(header, record);
        auto m = probe->auto_transform_measurement();
        wasmtime_val_t result = {.kind = WASMTIME_I32, .of = {.i32 = -1}};
        try {
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
            wasmtime_val_t param = {
              .kind = WASMTIME_I32, .of = {.i32 = fixed_input_record_handle}};
            is_executing = true;
            wasm_trap_t* trap_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_func_call(
                ctx,
                &cb.of.func,
                &param,
                /*params_size=*/1,
                &result,
                /*results_size=*/1,
                &trap_ptr)};
            handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
            is_executing = false;
            check_error(error.get());
            check_trap(trap.get());
        } catch (...) {
            _rp_module->post_call_unclean();
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
            _rp_module->post_call_unclean();
            probe->transform_error();
            throw wasm_exception(
              ss::format(
                "transform execution {} resulted in error {}",
                _user_module_name,
                result.of.i32),
              errc::user_code_failure);
        }
        co_return _rp_module->post_call();
    }

    ss::sstring _user_module_name;
    wasm_engine_t* _engine;
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    std::shared_ptr<wasmtime_module_t> _user_module;
    std::unique_ptr<redpanda_module> _rp_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
    handle<wasmtime_linker_t, wasmtime_linker_delete> _linker;
    wasmtime_instance_t _instance;
};

class wasmtime_engine_factory : public engine::factory {
public:
    wasmtime_engine_factory(
      wasm_engine_t* engine,
      std::shared_ptr<wasmtime_module_t> mod,
      ss::sstring user_module_name)
      : _engine(engine)
      , _module(std::move(mod))
      , _user_module_name(std::move(user_module_name)) {}

    std::unique_ptr<engine> make_engine() final {
        handle<wasmtime_store_t, wasmtime_store_delete> store{
          wasmtime_store_new(_engine, nullptr, nullptr)};
        auto* context = wasmtime_store_context(store.get());
        handle<wasmtime_linker_t, wasmtime_linker_delete> linker{
          wasmtime_linker_new(_engine)};

        auto rp_module = std::make_unique<redpanda_module>();
        register_rp_module(rp_module.get(), linker.get());

        auto wasi_module = std::make_unique<wasi::preview1_module>();
        register_wasi_module(wasi_module.get(), linker.get());

        wasmtime_instance_t instance;
        wasm_trap_t* trap_ptr = nullptr;
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_linker_instantiate(
            linker.get(), context, _module.get(), &instance, &trap_ptr));
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        check_error(error.get());
        check_trap(trap.get());

        return std::make_unique<wasmtime_engine>(
          _user_module_name,
          _engine,
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
    ss::sstring _user_module_name;
};

} // namespace

class runtime {
public:
    explicit runtime(wasm_engine_t* e)
      : _engine(e) {}

    wasm_engine_t* get() const { return _engine.get(); }

private:
    handle<wasm_engine_t, wasm_engine_delete> _engine;
};

std::unique_ptr<runtime> make_runtime() {
    wasm_config_t* config = wasm_config_new();

    wasmtime_config_cranelift_opt_level_set(config, WASMTIME_OPT_LEVEL_NONE);
    // TODO: Enable fuel consumption
    wasmtime_config_consume_fuel_set(config, false);
    wasmtime_config_wasm_bulk_memory_set(config, true);
    wasmtime_config_parallel_compilation_set(config, false);
    wasmtime_config_max_wasm_stack_set(config, 8_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);

    return std::make_unique<runtime>(wasm_engine_new_with_config(config));
}

bool is_running() { return is_executing; }

std::unique_ptr<engine::factory> compile(
  runtime* rt,
  std::string_view wasm_module_name,
  std::string_view wasm_source) {
    // TODO: This should probably just be another class somewhere that lives on
    // shard0 of the wasm service.
    wasmtime_module_t* user_module_ptr = nullptr;
    handle<wasmtime_error_t, wasmtime_error_delete> error{wasmtime_module_new(
      rt->get(),
      reinterpret_cast<const uint8_t*>(wasm_source.data()),
      wasm_source.size(),
      &user_module_ptr)};
    std::shared_ptr<wasmtime_module_t> user_module{
      user_module_ptr, wasmtime_module_delete};
    check_error(error.get());
    return std::make_unique<wasmtime_engine_factory>(
      rt->get(), user_module, ss::sstring(wasm_module_name));
}
} // namespace wasm::wasmtime
