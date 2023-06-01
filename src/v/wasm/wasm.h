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

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <utility>
#include <vector>

namespace wasm {

class probe;

class engine {
public:
    virtual ss::future<model::record_batch>
    transform(model::record_batch&& batch, probe* probe) = 0;

    virtual std::string_view function_name() const = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    engine() = default;
    virtual ~engine() = default;
    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;
    engine(engine&&) = default;
    engine& operator=(engine&&) = default;
};

struct transform {
    struct metadata {
        ss::sstring function_name;
        model::topic_namespace input;
        model::topic_namespace output;
    };
    metadata meta;
    std::unique_ptr<engine> engine;
};

class service {
public:
    service();

    ~service();
    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = default;
    service& operator=(service&&) = default;

    ss::future<> stop();

    model::record_batch_reader wrap_batch_reader(
      const model::topic_namespace_view&, model::record_batch_reader);

    std::optional<model::topic_namespace>
    wasm_transform_output_topic(const model::topic_namespace_view&) const;

    std::vector<transform::metadata> list_transforms() const;

    transform swap_transform(transform transform) {
        auto& current = _transforms[transform.meta.input];
        return std::exchange(current, std::move(transform));
    }
    transform remove_transform(const transform::metadata& meta) {
        auto it = _transforms.find(meta.input);
        auto removed = std::move(it->second);
        _transforms.erase(it);
        return removed;
    }

private:
    ss::gate _gate;
    std::unique_ptr<probe> _probe;

    absl::flat_hash_map<
      model::topic_namespace,
      transform,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _transforms;
};

ss::future<std::unique_ptr<engine>> make_wasm_engine(
  std::string_view wasm_module_name, std::string_view wasm_source);

} // namespace wasm
