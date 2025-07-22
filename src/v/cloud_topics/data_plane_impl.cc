/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/data_plane_impl.h"

#include "base/outcome.h"
#include "cloud_storage/cache_service.h"
#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/cluster_services.h"
#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/level_zero/batcher/batcher.h"
#include "cloud_topics/level_zero/reconciler/reconciler.h"
#include "cloud_topics/read_path/fetch_request_handler.h"
#include "cloud_topics/throttler/throttler.h"
#include "model/fundamental.h"
#include "storage/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>

namespace experimental::cloud_topics {

class impl
  : public data_plane_api
  , public ss::enable_shared_from_this<impl> {
public:
    impl(
      seastar::sharded<cluster::partition_manager>* pm,
      seastar::sharded<cloud_io::remote>* io,
      seastar::sharded<cloud_storage::cache>* cache,
      cloud_storage_clients::bucket_name bucket,
      seastar::sharded<storage::api>* storage_api,
      std::unique_ptr<cluster_services> cluster_services)
      : _cluster_services(std::move(cluster_services))
      , _reconciler(std::make_unique<reconciler::reconciler>(pm, io, bucket))
      , _write_pipeline(std::make_unique<core::write_pipeline<>>())
      , _throttler(std::make_unique<throttler<>>(
          10_MiB /*TODO: fixme*/,
          _write_pipeline->register_write_pipeline_stage()))
      , _batcher(std::make_unique<batcher<>>(
          _write_pipeline->register_write_pipeline_stage(),
          bucket,
          io->local(),
          _cluster_services.get()))
      , _read_pipeline(std::make_unique<core::read_pipeline<>>())
      , _l0_resolver(std::make_unique<fetch_handler>(
          _read_pipeline->register_read_pipeline_stage(),
          bucket,
          &io->local(),
          &cache->local()))
      , _batch_cache(
          std::make_unique<batch_cache>(&storage_api->local().log_mgr())) {}

    seastar::future<> start() override {
        // Batcher
        co_await _batch_cache->start();
        // Reconciler
        co_await _reconciler->start();
        // Write path
        co_await _throttler->start();
        co_await _batcher->start();
        // Read path
        co_await _l0_resolver->start();
    }
    seastar::future<> stop() override {
        // Read path
        co_await _read_pipeline->stop();
        co_await _l0_resolver->stop();
        // Write path
        co_await _write_pipeline->stop();
        co_await _batcher->stop();
        co_await _throttler->stop();
        //  Reconciler
        co_await _reconciler->stop();
        // Batcher
        co_await _batch_cache->stop();
    }

    ss::future<result<chunked_vector<extent_meta>>> write_and_debounce(
      model::ntp ntp,
      chunked_vector<model::record_batch> r,
      model::timeout_clock::time_point timeout) override {
        return _write_pipeline->write_and_debounce(
          std::move(ntp), std::move(r), timeout);
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      model::timeout_clock::time_point timeout) override {
        auto res = co_await _read_pipeline->make_reader(
          ntp,
          {.output_size_estimate = output_size_estimate,
           .meta = std::move(metadata)},
          timeout);
        if (!res) {
            co_return res.error();
        }
        co_return std::move(res.value().results);
    }

    void cache_put(const model::ntp& ntp, const model::record_batch& b) final {
        _batch_cache->put(ntp, b);
    }

    std::optional<model::record_batch>
    cache_get(const model::ntp& ntp, model::offset o) final {
        return _batch_cache->get(ntp, o);
    }

private:
    std::unique_ptr<cluster_services> _cluster_services;
    std::unique_ptr<reconciler::reconciler> _reconciler;
    // Write path
    std::unique_ptr<core::write_pipeline<>> _write_pipeline;
    std::unique_ptr<throttler<>> _throttler;
    std::unique_ptr<batcher<>> _batcher;
    // Read path
    std::unique_ptr<core::read_pipeline<>> _read_pipeline;
    std::unique_ptr<fetch_handler> _l0_resolver;
    // Batch cache
    std::unique_ptr<batch_cache> _batch_cache;
};

ss::shared_ptr<data_plane_api> make_data_plane(
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_storage::cache>* cache,
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<storage::api>* log_manager,
  std::unique_ptr<cluster_services> cluster_services) {
    return ss::make_shared<impl>(
      partition_manager,
      remote,
      cache,
      std::move(bucket),
      log_manager,
      std::move(cluster_services));
}

} // namespace experimental::cloud_topics
