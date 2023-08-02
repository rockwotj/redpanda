#pragma once

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

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

    virtual std::optional<std::unique_ptr<T>> create(model::ntp) = 0;
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

    virtual ss::future<> write(ss::chunked_fifo<model::record_batch>) = 0;

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

    virtual cluster::notification_id_type
      register_on_write_notification(ss::noncopyable_function<void()>)
      = 0;
    virtual void unregister_on_write_notification(cluster::notification_id_type)
      = 0;
    virtual ss::future<model::offset> load_latest_offset() = 0;
    virtual ss::future<model::record_batch_reader>
    read_batch(model::offset, ss::abort_source*) = 0;

    using factory = factory<source>;
};
} // namespace transform
