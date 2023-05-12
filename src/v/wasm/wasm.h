

#pragma once

#include "errc.h"
#include "kafka/protocol/batch_reader.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "outcome.h"
#include "seastarx.h"
#include <seastar/core/gate.hh>

#include <memory>
#include <vector>

namespace wasm {

class engine {
public:
    virtual ss::future<model::record_batch>
    transform(model::record_batch&& batch) = 0;

    engine() = default;
    virtual ~engine() = default;
    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;
    engine(engine&&) = default;
    engine& operator=(engine&&) = default;

};

class service {
  public:
    service() = default;

    ~service() = default;
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = default;
    service& operator=(service&&) = default;

    bool is_enabled() const { return bool(_engine); }

    ss::future<> stop();

    model::record_batch_reader wrap_batch_reader(model::record_batch_reader);
  
    void swap_engine(std::unique_ptr<engine>& engine) {
      engine.swap(engine);
    }

  private:
    ss::gate _gate;
    std::unique_ptr<engine> _engine;
};

ss::future<std::unique_ptr<engine>> make_wasm_engine(std::string_view wasm_source);

} // namespace wasm
