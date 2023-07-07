#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"

#include <memory>

namespace transform {

template<typename T>
class factory {
public:
    factory() = default;
    factory(const factory&) = delete;
    factory& operator=(const factory&) = delete;
    factory(factory&&) = delete;
    factory& operator=(factory&&) = delete;
    virtual ~factory() = default;

    virtual std::unique_ptr<T> create(model::ntp) = 0;
};

/**
 * The output sink for wasm transforms.
 */
class sink {
public:
    sink() = default;
    sink(const sink&) = delete;
    sink& operator=(const sink&) = delete;
    sink(sink&&) = delete;
    sink& operator=(sink&&) = delete;
    virtual ~sink() = default;

    virtual ss::future<> write(model::record_batch) = 0;

    using factory = factory<sink>;
};

/**
 * The input source for wasm transforms.
 */
class source {
public:
    source() = default;
    source(const source&) = delete;
    source& operator=(const source&) = delete;
    source(source&&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    virtual ss::future<model::offset> load_latest_offset() = 0;
    virtual ss::future<std::optional<model::record_batch_reader>>
      read_batch(model::offset) = 0;

    using factory = factory<source>;
};
} // namespace transform
