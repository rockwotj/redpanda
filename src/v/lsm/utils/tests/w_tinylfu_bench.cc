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
#include "lsm/utils/w_tinylfu.h"
#include "random/generators.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>

#include <random>
#include <string>

using namespace lsm::utils;

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
PERF_TEST(w_tinylfu, insert_sequential_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

PERF_TEST(w_tinylfu, insert_sequential_10k) {
    w_tinylfu_cache<int, int> cache({.capacity = 10000});
    perf_tests::start_measuring_time();
    for (int i = 0; i < 10000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 10000;
}

// Benchmark: Random lookups (hits)
PERF_TEST(w_tinylfu, find_hit_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(0, 999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.find(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Random lookups (misses)
PERF_TEST(w_tinylfu, find_miss_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(1000, 1999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.find(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Mixed workload (80% hits, 20% misses)
PERF_TEST(w_tinylfu, mixed_workload_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(
      0, 1249); // 80% in [0,999], 20% in [1000,1249]

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        auto result = cache.find(dis(gen));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Zipfian distribution (realistic cache workload)
PERF_TEST(w_tinylfu, zipfian_workload_10k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
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
        auto result = cache.find(key);
        if (!result) {
            cache.insert(key, ss::make_lw_shared<int>(key));
        }
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    auto stats = cache.statistics();
    fmt::print(
      stderr,
      "hit ratio: {}\n",
      stats.hits / static_cast<double>(stats.hits + stats.misses));
    return 10000;
}

// Benchmark: Insert with eviction
PERF_TEST(w_tinylfu, insert_with_eviction_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 2000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 2000;
}

// Benchmark: Erase operations
PERF_TEST(w_tinylfu, erase_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.erase(i);
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Update existing keys
PERF_TEST(w_tinylfu, update_1k) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i * 2));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: String keys (more realistic)
PERF_TEST(w_tinylfu, string_keys_insert_1k) {
    w_tinylfu_cache<std::string, int> cache({.capacity = 1000});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        std::string key = absl::StrCat("key_", i);
        cache.insert(key, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 100;
}

PERF_TEST(w_tinylfu, string_keys_find_1k) {
    w_tinylfu_cache<std::string, int> cache({.capacity = 1000});
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        cache.insert(key, ss::make_lw_shared<int>(i));
    }

    std::mt19937 gen(42);
    std::uniform_int_distribution<> dis(0, 999);

    perf_tests::start_measuring_time();
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(dis(gen));
        auto result = cache.find(key);
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return 1000;
}

// Benchmark: Aging mechanism (frequency sketch reset)
PERF_TEST(w_tinylfu, aging_trigger) {
    w_tinylfu_cache<int, int> cache({.capacity = 100, .sample_size = 1000});

    perf_tests::start_measuring_time();
    // Trigger aging by exceeding sample_size
    for (int i = 0; i < 1100; ++i) {
        cache.insert(i % 200, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 1100;
}

// Benchmark: Large cache
PERF_TEST(w_tinylfu, large_cache_100k) {
    w_tinylfu_cache<int, int> cache({.capacity = 100000});

    perf_tests::start_measuring_time();
    for (int i = 0; i < 100000; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        perf_tests::do_not_optimize(cache);
    }
    perf_tests::stop_measuring_time();
    return 100000;
}
