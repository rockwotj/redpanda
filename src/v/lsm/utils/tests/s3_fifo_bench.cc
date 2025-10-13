/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/strings/str_cat.h"
#include "random/generators.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/core.h>

#include <random>
#include <string>

// Zipfian distribution generator for realistic cache access patterns
class zipfian_generator {
public:
    explicit zipfian_generator(size_t n, double alpha = 0.99)
      : n_(n)
      , alpha_(alpha)
      , gen_(random_generators::global()) {
        // Precompute normalization constant
        double sum = 0.0;
        for (size_t i = 1; i <= n; ++i) {
            sum += 1.0 / std::pow(i, alpha);
        }
        c_ = 1.0 / sum;
    }

    size_t operator()() {
        auto z = gen_.get_real<double>();
        double cum = 0.0;
        for (size_t i = 1; i <= n_; ++i) {
            cum += c_ / std::pow(i, alpha_);
            if (cum >= z) {
                return i - 1;
            }
        }
        return n_ - 1;
    }

private:
    size_t n_;
    double alpha_;
    double c_;
    random_generators::rng& gen_;
};

// Benchmark: Sequential inserts
PERF_TEST(s3_fifo, insert_sequential_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

PERF_TEST(s3_fifo, insert_sequential_10k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 10000, .small_size = 1000});
    perf_tests::start_measuring_time();
    for (int i = 0; i < 10000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 10000;
}

// Benchmark: Random lookups (hits)
PERF_TEST(s3_fifo, find_hit_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(0, 999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.get_value(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Random lookups (misses)
PERF_TEST(s3_fifo, find_miss_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(1000, 1999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.get_value(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Mixed workload (80% hits, 20% misses)
PERF_TEST(s3_fifo, mixed_workload_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(
      0, 1249); // 80% in [0,999], 20% in [1000,1249]

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.get_value(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Zipfian distribution (realistic cache workload)
PERF_TEST(s3_fifo, zipfian_workload_10k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    // Compute the distribution outside the benchmark
    zipfian_generator gen(10000);
    std::vector<int> keys;
    keys.resize(10000);
    for (int& k : keys) {
        k = gen();
    }

    perf_tests::start_measuring_time();
    for (int i = 0; i < 10000; ++i) {
        int key = keys[i];
        auto result = cache.get_value(key);
        if (!result) {
            cache.try_insert(key, ss::make_shared<int>(key));
        }
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    auto stats = cache.stat();
    fmt::print(
      stderr,
      "hit ratio: {}\n",
      stats.hit_count / static_cast<double>(stats.access_count));
    return 10000;
}

// Benchmark: Insert with eviction
PERF_TEST(s3_fifo, insert_with_eviction_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 2000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 2000;
}

// Benchmark: Erase operations
PERF_TEST(s3_fifo, erase_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
    }

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.evict(i);
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Update existing keys
PERF_TEST(s3_fifo, update_1k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
    }

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.evict(i);
        cache.try_insert(i, ss::make_shared<int>(i * 2));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: String keys (more realistic)
PERF_TEST(s3_fifo, string_keys_insert_1k) {
    utils::chunked_kv_cache<std::string, int> cache(
      {.cache_size = 1000, .small_size = 100});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        std::string key = absl::StrCat("key_", i);
        cache.try_insert(key, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 100;
}

PERF_TEST(s3_fifo, string_keys_find_1k) {
    utils::chunked_kv_cache<std::string, int> cache(
      {.cache_size = 1000, .small_size = 100});
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        cache.try_insert(key, ss::make_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(0, 999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(dis(gen));
        auto result = cache.get_value(key);
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Aging mechanism equivalent (frequent evictions)
PERF_TEST(s3_fifo, aging_trigger) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 100, .small_size = 10});

    perf_tests::start_measuring_time();
    // Trigger frequent evictions by exceeding capacity
    for (int i = 0; i < 1100; ++i) {
        cache.try_insert(i % 200, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1100;
}

// Benchmark: Large cache
PERF_TEST(s3_fifo, large_cache_100k) {
    utils::chunked_kv_cache<int, int> cache(
      {.cache_size = 100000, .small_size = 10000});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 100000; ++i) {
        cache.try_insert(i, ss::make_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 100000;
}
