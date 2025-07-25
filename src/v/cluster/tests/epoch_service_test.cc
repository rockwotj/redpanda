// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_epoch_service.h"
#include "gmock/gmock.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

using epoch_service = cluster::cluster_epoch_service<ss::manual_clock>;

class ClusterEpochService : public seastar_test {
protected:
    ss::future<> SetUpAsync() override {
        co_await service.start(ss::sharded_parameter([this] {
            return [this](ss::manual_clock::duration) {
                ++accesses;
                if (injected_failure_count > 0) {
                    --injected_failure_count;
                    throw std::runtime_error("Fetch failed");
                }
                return ss::as_ready_future(cluster_epoch.load());
            };
        }));
        co_await service.invoke_on_all(&epoch_service::start);
    }
    ss::future<> TearDownAsync() override { co_await service.stop(); }

    std::atomic<int64_t> cluster_epoch = 0;
    std::atomic<int64_t> accesses = 0;
    std::atomic<int64_t> injected_failure_count = 0;
    ss::sharded<epoch_service> service;
};

TEST_F_CORO(ClusterEpochService, TestCaching) {
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 1);
    ++cluster_epoch;
    // Value is cached
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch - 1);
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 1);
    // After the timeout we async re-fetch the value
    ss::manual_clock::advance(epoch_service::epoch_cache_timeout + 1us);
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch - 1);
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 2);
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 2);
    // After the max duration we wait to fetch the value
    ++cluster_epoch;
    ss::manual_clock::advance(
      epoch_service::max_same_epoch_cache_duration + 1us);
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 3);
}

TEST_F_CORO(ClusterEpochService, IncrementMustHappenEventually) {
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 1);
    auto must_refresh_deadline = ss::manual_clock::now()
                                 + epoch_service::max_same_epoch_cache_duration;
    // After the timeout we async re-fetch the value
    ss::manual_clock::advance(epoch_service::epoch_cache_timeout + 1us);
    while (ss::manual_clock::now() < must_refresh_deadline) {
        auto accesses_before = accesses.load();
        EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
        co_await tests::drain_task_queue();
        EXPECT_EQ(accesses, accesses_before + 1);
        ss::manual_clock::advance(epoch_service::epoch_cache_timeout + 1us);
    }
    // After the max duration we wait to fetch the value, and will fail if we
    // cannot
    EXPECT_ANY_THROW(co_await service.local().get_cached_epoch());
    EXPECT_ANY_THROW(co_await service.local().get_cached_epoch());
    // After the epoch is updated we can fetch again successfully
    ++cluster_epoch;
    auto accesses_before = accesses.load();
    EXPECT_EQ(co_await service.local().get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, accesses_before + 1);
}

TEST_F_CORO(ClusterEpochService, FetchesLimitedToShard0) {
    using ::testing::ElementsAre;
    auto all_epochs = [this]() {
        return service.map(
          [](epoch_service& es) { return es.get_cached_epoch(); });
    };
    EXPECT_THAT(co_await all_epochs(), ElementsAre(0, 0));
    EXPECT_EQ(accesses, 1);
    // Bumping the epoch and awaiting for the cache to expire should only cause
    // one additional fetch
    ++cluster_epoch;
    ss::manual_clock::advance(epoch_service::epoch_cache_timeout + 1us);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(0, 0));
    // Drain the queue twice to ensure that cross shard tasks are executed
    // and complete.
    co_await tests::drain_task_queue();
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 2);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(1, 1));
}

TEST_F_CORO(ClusterEpochService, CacheInvalidation) {
    using ::testing::ElementsAre;
    ++cluster_epoch;
    auto all_epochs = [this]() {
        return service.map(
          [](epoch_service& es) { return es.get_cached_epoch(); });
    };
    EXPECT_THAT(co_await all_epochs(), ElementsAre(1, 1));
    EXPECT_EQ(accesses, 1);
    ++cluster_epoch;
    // Lower epoch does not invalidate anything
    co_await service.invoke_on_all(&epoch_service::invalidate_epoch_cache, 0);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(1, 1));
    co_await tests::drain_task_queue();
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 1);
    co_await service.invoke_on_all(&epoch_service::invalidate_epoch_cache, 2);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(2, 2));
    EXPECT_EQ(accesses, 2);
}
