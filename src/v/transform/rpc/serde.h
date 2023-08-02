/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"

#include <seastar/core/chunked_fifo.hh>

#include <absl/container/flat_hash_map.h>

namespace transform::rpc {

struct transformed_partition_data
  : serde::envelope<
      transformed_partition_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    transformed_partition_data() = default;
    transformed_partition_data(
      model::partition_id p, ss::chunked_fifo<model::record_batch> b)
      : partition_id(p)
      , batches{std::move(b)} {}
    model::partition_id partition_id;
    ss::chunked_fifo<model::record_batch> batches;

    auto serde_fields() { return std::tie(partition_id, batches); }
};

struct transformed_topic_data
  : serde::envelope<
      transformed_topic_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    transformed_topic_data() = default;
    transformed_topic_data(model::topic_partition, model::record_batch);
    transformed_topic_data(
      model::topic_partition, ss::chunked_fifo<model::record_batch>);
    transformed_topic_data(
      model::topic t, ss::chunked_fifo<transformed_partition_data> d)
      : topic(std::move(t))
      , partitions{std::move(d)} {}

    model::topic topic;
    ss::chunked_fifo<transformed_partition_data> partitions;

    auto serde_fields() { return std::tie(topic, partitions); }
};

struct produce_request
  : serde::
      envelope<produce_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    produce_request() = default;
    produce_request(
      ss::chunked_fifo<transformed_topic_data> topic_data,
      model::timeout_clock::duration timeout)
      : topic_data{std::move(topic_data)}
      , timeout{timeout} {}

    auto serde_fields() { return std::tie(topic_data, timeout); }

    ss::chunked_fifo<transformed_topic_data> topic_data;
    model::timeout_clock::duration timeout{};
};

struct transformed_partition_data_result
  : serde::envelope<
      transformed_partition_data_result,
      serde::version<0>,
      serde::compat_version<0>> {
    transformed_partition_data_result() = default;
    transformed_partition_data_result(
      model::topic_partition tp, cluster::errc ec)
      : tp(std::move(tp))
      , err(ec) {}

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};

    auto serde_fields() { return std::tie(tp, err); }
};

struct produce_reply
  : serde::
      envelope<produce_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    produce_reply() = default;
    explicit produce_reply(
      ss::chunked_fifo<transformed_partition_data_result> r)
      : results(std::move(r)) {}

    auto serde_fields() { return std::tie(results); }

    ss::chunked_fifo<transformed_partition_data_result> results;
};

} // namespace transform::rpc
