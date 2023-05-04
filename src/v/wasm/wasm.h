

#pragma once

#include "errc.h"
#include "kafka/protocol/batch_reader.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "outcome.h"
#include "seastarx.h"
#include <seastar/core/gate.hh>

#include <vector>

namespace wasm {

class engine {
public:
    // TODO: How akward is this API? Can we flatten the batches.
    virtual ss::future<std::vector<model::record_batch>>
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
    explicit service(std::unique_ptr<engine>);

    ~service() = default;
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = default;
    service& operator=(service&&) = default;

    bool is_enabled() const { return bool(_engine); }

    ss::future<> stop();

    model::record_batch_reader wrap_batch_reader(model::record_batch_reader);

  private:
    ss::gate _gate;
    std::unique_ptr<engine> _engine;
};

result<std::unique_ptr<engine>, errc> make_wasm_engine(std::string_view wasm_source);

} // namespace wasm
