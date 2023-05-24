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
#include "bytes/bytes.h"
#include "wasm/errc.h"
#include "http/client.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/timestamp.h"
#include "net/tls.h"
#include "net/unresolved_address.h"
#include "outcome.h"
#include "seastarx.h"
#include "ssx/metrics.h"
#include "utils/hdr_hist.h"
#include "utils/mutex.h"
#include "utils/uri.h"
#include "utils/vint.h"
#include "wasm/ffi.h"
#include "wasm/wasmtime.h"

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

service::service() : _gate(), _probe(std::make_unique<probe>()), _engines() {}

service::~service() = default;

ss::future<> service::stop() { return _gate.close(); }

model::record_batch_reader
service::wrap_batch_reader(const model::topic_namespace& nt, model::record_batch_reader batch_reader) {
    auto it = _engines.find(nt);
    if (it != _engines.end()) {
        return model::make_record_batch_reader<wasm_transform_applying_reader>(
          std::move(batch_reader), it->second.get(), _probe.get(), _gate.hold());
    }
    return batch_reader;
}

ss::future<std::unique_ptr<engine>>
make_wasm_engine(std::string_view wasm_module_name, std::string_view wasm_source) {
    co_return co_await wasmtime::make_wasm_engine(wasm_module_name, wasm_source);
}

} // namespace wasm
