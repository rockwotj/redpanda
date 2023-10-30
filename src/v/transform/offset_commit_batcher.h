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
#include "model/fundamental.h"
#include "model/transform.h"
#include "outcome.h"
#include "transform/io.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>

namespace transform {

/**
 * An interface for the offset tracking KV store.
 *
 * It supports determining the owner of a given key, and then batch commiting
 * values for a partition within the KV store.
 */
class offset_committer {
public:
    offset_committer() = default;
    offset_committer(const offset_committer&) = default;
    offset_committer(offset_committer&&) = default;
    offset_committer& operator=(const offset_committer&) = default;
    offset_committer& operator=(offset_committer&&) = default;
    virtual ~offset_committer() = default;

    /**
     * Lookup which partition owns a given key.
     */
    virtual ss::future<result<model::partition_id, cluster::errc>>
      find_coordinator(model::transform_offsets_key) = 0;

    /**
     * Commit a batch of data for a partition.
     *
     * The partition **must** be owner of every key in the batch.
     */
    virtual ss::future<cluster::errc> batch_commit(
      model::partition_id coordinator,
      absl::
        btree_map<model::transform_offsets_key, model::transform_offsets_value>)
      = 0;
};

template<typename ClockType = ss::lowres_clock>
class offset_commit_batcher {
    static_assert(
      std::is_same_v<ClockType, ss::lowres_clock>
        || std::is_same_v<ClockType, ss::manual_clock>,
      "Only lowres or manual clocks are supported");

public:
    explicit offset_commit_batcher(
      ClockType::duration commit_interval, std::unique_ptr<offset_committer>);

    ss::future<> start();
    ss::future<> stop();

    /**
     * Preload the coordinator information for a given key.
     *
     * This is not a requirement as partitions can change and internally the
     * batcher will handle reloading the partition for a key.
     */
    ss::future<> preload(model::transform_offsets_key);
    /**
     * Unload the coordinator information as it's not going to be needed anymore
     * (no more calls to `commit_offset` will be called on this core).
     *
     * This is a used instead of a cache eviction algorithm so it's important
     * that users of this class call this to unload the key and reduce memory
     * usage.
     */
    ss::future<> unload(model::transform_offsets_key);

    /**
     * Enqueue a commit to be batched with other commits.
     */
    ss::future<> commit_offset(
      model::transform_offsets_key, model::transform_offsets_value);

    /**
     * Flush all of the pending commits to the underlying offset_committer.
     */
    ss::future<> flush();

private:
    ss::future<> find_coordinator_loop();
    /**
     * Return true if all requests to find coordinators failed.
     *
     * This can be used by the coordinator loop to backoff if requests are
     * failing instead of retrying in a loop.
     */
    ss::future<bool> assign_coordinators();

    using kv_map = absl::
      btree_map<model::transform_offsets_key, model::transform_offsets_value>;

    kv_map _unbatched;
    absl::btree_map<model::partition_id, kv_map> _batched;
    absl::btree_map<model::transform_offsets_key, model::partition_id>
      _coordinator_cache;

    ss::condition_variable _unbatched_cond_var;
    std::unique_ptr<offset_committer> _offset_committer;
    ClockType::duration _commit_interval;
    ss::timer<ClockType> _timer;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace transform
