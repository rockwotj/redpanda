/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "errc.h"
#include "kafka/protocol/batch_reader.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "outcome.h"
#include "seastarx.h"
#include "ssx/thread_worker.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <utility>
#include <vector>

namespace wasm {

namespace wasmedge {
struct runtime;
} // namespace wasmedge
class probe;

class engine {
public:
    virtual ss::future<model::record_batch>
    transform(const model::record_batch* batch, probe* probe) = 0;

    virtual std::string_view function_name() const = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    engine() = default;
    virtual ~engine() = default;
    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;
    engine(engine&&) = default;
    engine& operator=(engine&&) = default;

    class factory {
    public:
        factory() = default;
        factory(const factory&) = delete;
        factory& operator=(const factory&) = delete;
        factory(factory&&) = delete;
        factory& operator=(factory&&) = delete;
        virtual std::unique_ptr<engine> make_engine() = 0;
        virtual ~factory() = default;
    };
};

struct transform {
    struct metadata {
        ss::sstring function_name;
        model::topic_namespace input;
        model::topic_namespace output;
    };
    metadata meta;
    std::unique_ptr<engine> engine;
    std::unique_ptr<probe> probe;
};

class service : public ss::peering_sharded_service<service> {
public:
    explicit service(ssx::thread_worker*);

    ~service();
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = delete;
    service& operator=(service&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    model::record_batch_reader wrap_batch_reader(
      const model::topic_namespace_view&, model::record_batch_reader);

    std::optional<model::topic_namespace>
    wasm_transform_output_topic(const model::topic_namespace_view&) const;

    std::vector<transform::metadata> list_transforms() const;

    /**
     * This is a wasmtime hack!
     *
     * We need to ignore some signals as wasmtime intentionally triggers some
     * signals to make these happen.
     */
    void install_signal_handlers();

    ss::future<> deploy_transform(transform::metadata, ss::sstring source);

    ss::future<> undeploy_transform(const transform::metadata&);

private:
    ss::gate _gate;
    ssx::thread_worker* _worker;

    std::unique_ptr<wasmedge::runtime, void (*)(wasmedge::runtime*)> _runtime;
    absl::flat_hash_map<
      model::topic_namespace,
      transform,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _transforms;
};

} // namespace wasm
