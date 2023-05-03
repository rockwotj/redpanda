

#pragma once

#include "errc.h"
#include "model/record.h"
#include "outcome.h"
#include "seastarx.h"

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

result<std::unique_ptr<engine>, errc> make_wasm_engine(ss::sstring wasm_source);

} // namespace wasm
