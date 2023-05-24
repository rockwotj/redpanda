#include "wasmtime.h"

#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "utils/mutex.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/probe.h"
#include "wasm/wasi.h"
#include "wasmstar.h"

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>

namespace wasm::wasmtime {

namespace {

rust::Str str_from_string_view(std::string_view sv) {
    return {sv.data(), sv.size()};
}

static ss::logger wasmtime_log("wasmtime");

using read_result = int32_t;
using write_result = int32_t;
using input_record_handle = int32_t;
using output_record_handle = int32_t;

// Right now we only ever will have a single handle
constexpr input_record_handle fixed_input_record_handle = 1;
constexpr size_t max_output_records = 256;

constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";

struct record_builder {
    std::optional<iobuf> key;
    std::optional<iobuf> value;
    std::vector<model::record_header> headers;
};

struct transform_context {
    iobuf_parser key;
    iobuf_parser value;
    model::offset offset;
    model::timestamp timestamp;

    std::vector<model::record_header> headers;

    model::record_attributes record_attributes;
    int64_t timestamp_delta;
    int32_t offset_delta;
    std::vector<record_builder> output_records;
};

rust::Vec<wasmstar::ValueType>
convert_to_wasmtime(const std::vector<ffi::val_type>& ffi_types) {
    rust::Vec<wasmstar::ValueType> wasmstar_types;
    wasmstar_types.reserve(ffi_types.size());
    for (auto ty : ffi_types) {
        switch (ty) {
        case ffi::val_type::i32:
            wasmstar_types.push_back(wasmstar::ValueType::I32);
            break;
        case ffi::val_type::i64:
            wasmstar_types.push_back(wasmstar::ValueType::I64);
            break;
        case ffi::val_type::f32:
            wasmstar_types.push_back(wasmstar::ValueType::F32);
            break;
        case ffi::val_type::f64:
            wasmstar_types.push_back(wasmstar::ValueType::F64);
            break;
        }
    }
    return wasmstar_types;
}

class memory : public ffi::memory {
public:
    explicit memory(rust::Slice<uint8_t> mem)
      : ffi::memory()
      , _underlying(mem) {}

    void* translate(size_t guest_ptr, size_t len) final {
        if ((guest_ptr + len) > _underlying.size()) [[unlikely]] {
            throw wasm_exception(
              ss::format(
                "Out of bounds memory access in FFI: {} + {} >= {}",
                guest_ptr,
                len,
                _underlying.size()),
              errc::user_code_failure);
        }
        return _underlying.data() + guest_ptr;
    }

private:
    rust::Slice<uint8_t> _underlying;
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
      wasmstar::Linker& linker,
      Module* host_module,
      std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        rust::Vec<wasmstar::ValueType> inputs = convert_to_wasmtime(ffi_inputs);
        rust::Vec<wasmstar::ValueType> outputs = convert_to_wasmtime(
          ffi_outputs);

        auto host_func = std::make_unique<wasmstar::HostFunc>(
          [host_module](
            rust::Slice<uint8_t> raw_memory,
            rust::Slice<const uint64_t> guest_params,
            rust::Slice<uint64_t> guest_results) {
              memory mem(raw_memory);
              auto host_params = ffi::extract_parameters<ArgTypes...>(
                &mem, guest_params, 0);
              if constexpr (std::is_void_v<ReturnType>) {
                  std::apply(
                    module_func,
                    std::tuple_cat(std::make_tuple(host_module), host_params));
              } else {
                  ReturnType host_result = std::apply(
                    module_func,
                    std::tuple_cat(std::make_tuple(host_module), host_params));
                  guest_results[0] = static_cast<uint64_t>(host_result);
              }
          });

        linker.register_host_fn(
          str_from_string_view(Module::name),
          str_from_string_view(function_name),
          inputs,
          outputs,
          std::move(host_func));
    }
};

void register_wasi_module(
  wasi::preview1_module* mod, wasmstar::Linker& linker) {
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(linker, mod, #name)
    REG_HOST_FN(clock_time_get);
    REG_HOST_FN(args_get);
    REG_HOST_FN(args_sizes_get);
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
}

class redpanda_module {
public:
    redpanda_module() = default;
    redpanda_module(const redpanda_module&) = delete;
    redpanda_module& operator=(const redpanda_module&) = delete;
    redpanda_module(redpanda_module&&) = default;
    redpanda_module& operator=(redpanda_module&&) = default;
    ~redpanda_module() = default;

    static constexpr std::string_view name = "redpanda";

    void register_module(wasmstar::Linker& linker) {
#define REG_HOST_FN(name)                                                      \
    host_function<&redpanda_module::name>::reg(linker, this, #name)
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

    void
    prep_call(const model::record_batch_header& header, model::record& record) {
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

    void post_call_unclean() { _call_ctx = std::nullopt; }

    model::record_batch::uncompressed_records post_call() {
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
                        + vint::vint_size(h.value_size())
                        + h.value().size_bytes();
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

    // Start ABI exports
    // This is a small set just to get the ball rolling

    read_result read_key(input_record_handle handle, ffi::array<uint8_t> data) {
        if (handle != fixed_input_record_handle || !_call_ctx || !data) {
            return -1;
        }
        size_t remaining = _call_ctx->key.bytes_left();
        size_t amount = std::min(size_t(data.size()), remaining);
        _call_ctx->key.consume_to(amount, data.raw());
        return int32_t(amount);
    }

    read_result
    read_value(input_record_handle handle, ffi::array<uint8_t> data) {
        if (handle != fixed_input_record_handle || !_call_ctx || !data) {
            return -1;
        }
        size_t remaining = _call_ctx->value.bytes_left();
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

private:
    std::optional<transform_context> _call_ctx;
};

class wasmtime_engine : public engine {
public:
    static ss::future<std::unique_ptr<wasmtime_engine>>
    make(std::string_view wasm_module_name, std::string_view wasm_source);

    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;

    ~wasmtime_engine() override = default;

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
            co_await initialize_wasi();
            _wasi_started = true;
        }

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

private:
    wasmtime_engine(
      std::string_view user_module_name,
      rust::Box<wasmstar::Engine> e,
      rust::Box<wasmstar::Linker> l,
      rust::Box<wasmstar::Store> s,
      rust::Box<wasmstar::Instance> i,
      std::unique_ptr<redpanda_module> rp_module,
      std::unique_ptr<wasi::preview1_module> wasi_module)
      : engine()
      , _user_module_name(user_module_name)
      , _engine(std::move(e))
      , _linker(std::move(l))
      , _store(std::move(s))
      , _instance(std::move(i))
      , _rp_module(std::move(rp_module))
      , _wasi_module(std::move(wasi_module)) {}

    ss::future<> initialize_wasi() {
        try {
            auto handle = _instance->lookup_function(
              *_store,
              str_from_string_view(wasi::preview_1_start_function_name),
              {},
              {});

            auto running = handle->invoke(*_store, {});

            while (!running->pump()) {
                co_await ss::coroutine::maybe_yield();
            }
        } catch (...) {
            // Get the right transform name here
            throw wasm_exception(
              ss::format(
                "wasi _start initialization {} failed: {}",
                _user_module_name,
                std::current_exception()),
              errc::user_code_failure);
        }
    }

    ss::future<std::vector<model::record>> invoke_transform(
      const model::record_batch_header& header,
      model::record& record,
      probe* probe) {
        _rp_module->prep_call(header, record);
        auto m = probe->auto_transform_measurement();
        rust::Vec<uint64_t> results;
        try {
            auto handle = _instance->lookup_function(
              *_store,
              str_from_string_view(redpanda_on_record_callback_function_name),
              {wasmstar::ValueType::I32},
              {wasmstar::ValueType::I32});
            std::array<uint64_t, 1> params{fixed_input_record_handle};
            auto running = handle->invoke(
              *_store, {params.data(), params.size()});
            while (!running->pump()) {
                co_await ss::coroutine::maybe_yield();
            }
            results = running->results();
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
        if (results.size() != 1) {
            _rp_module->post_call_unclean();
            probe->transform_error();
            throw wasm_exception(
              ss::format(
                "transform execution {} returned an invalid number of "
                "parameters: {}",
                _user_module_name,
                results.size()),
              errc::user_code_failure);
        }
        auto user_result = static_cast<int32_t>(results[0]);
        if (user_result < 0) {
            _rp_module->post_call_unclean();
            probe->transform_error();
            throw wasm_exception(
              ss::format(
                "transform execution {} resulted in error {}",
                _user_module_name,
                user_result),
              errc::user_code_failure);
        }
        co_return _rp_module->post_call();
    }

    std::string _user_module_name;
    mutex _mutex;
    // Should be global state:
    rust::Box<wasmstar::Engine> _engine;
    rust::Box<wasmstar::Linker> _linker;
    // Instance state:
    rust::Box<wasmstar::Store> _store;
    rust::Box<wasmstar::Instance> _instance;
    std::unique_ptr<redpanda_module> _rp_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
    bool _wasi_started = false;
};

ss::future<std::unique_ptr<wasmtime_engine>> wasmtime_engine::make(
  std::string_view wasm_module_name, std::string_view wasm_source) {
    // TODO: The engine should be a global object.
    auto engine = wasmstar::create_engine();

    auto store = engine->create_store();
    auto linker = engine->create_linker();

    auto rp_module = std::make_unique<redpanda_module>();
    rp_module->register_module(*linker);

    auto wasi_module = std::make_unique<wasi::preview1_module>();
    register_wasi_module(wasi_module.get(), *linker);

    auto user_module = engine->compile_module(rust::Slice<const uint8_t>(
      reinterpret_cast<const uint8_t*>(wasm_source.data()),
      wasm_source.size()));

    auto instance = store->create_instance(*linker, *user_module);
    instance->register_memory(*store);

    return ss::make_ready_future<std::unique_ptr<wasmtime_engine>>(
      std::unique_ptr<wasmtime_engine>(new wasmtime_engine(
        wasm_module_name,
        std::move(engine),
        std::move(linker),
        std::move(store),
        std::move(instance),
        std::move(rp_module),
        std::move(wasi_module))));
}
} // namespace

ss::future<std::unique_ptr<engine>> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source) {
    co_return co_await wasmtime_engine::make(wasm_module_name, wasm_source);
}
} // namespace wasm::wasmtime
