/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "wasm.h"

#include "wasm/probe.h"
#include "wasm/wasmedge.h"
#include "wasm/wasmtime.h"

namespace wasm {

namespace {

static ss::logger wasm_log("wasm");

class wasm_transform_applying_reader : public model::record_batch_reader::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    wasm_transform_applying_reader(
      model::record_batch_reader r,
      engine* engine,
      probe* probe,
      ss::gate::holder gate_holder)
      : _gate_holder(std::move(gate_holder))
      , _engine(engine)
      , _probe(probe)
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
                  std::move(batch), _probe);
                output.emplace_back(std::move(transformed));
            }
        } else {
            auto& d = std::get<foreign_data_t>(ret);
            for (auto& batch : *d.buffer) {
                auto transformed = co_await _engine->transform(
                  std::move(batch), _probe);
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
    probe* _probe;
    std::unique_ptr<model::record_batch_reader::impl> _source;
};

} // namespace

service::service()
  : _gate()
  , _probe(std::make_unique<probe>())
  , _engines() {}

service::~service() = default;

ss::future<> service::stop() { return _gate.close(); }

model::record_batch_reader service::wrap_batch_reader(
  const model::topic_namespace_view& nt, model::record_batch_reader batch_reader) {
    auto it = _engines.find(nt);
    if (it != _engines.end()) {
        return model::make_record_batch_reader<wasm_transform_applying_reader>(
          std::move(batch_reader),
          it->second.get(),
          _probe.get(),
          _gate.hold());
    }
    return batch_reader;
}

std::vector<live_wasm_function> service::list_engines() const {
  std::vector<live_wasm_function> functions;
  functions.reserve(_engines.size());
  for (auto& [tp_ns, engine] : _engines) {
    functions.emplace_back(ss::sstring(engine->function_name()), tp_ns);
  }
  return functions;
}

ss::future<std::unique_ptr<engine>> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source) {
    co_return co_await wasmtime::make_wasm_engine(
      wasm_module_name, wasm_source);
}

} // namespace wasm
