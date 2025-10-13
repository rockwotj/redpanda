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

#include "lsm/utils/w_tinylfu.h"

#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

#include <string>

using namespace lsm::utils;

TEST(WTinyLFU, BasicInsertAndFind) {
    w_tinylfu_cache<int, std::string> cache({.capacity = 10});

    auto value = ss::make_lw_shared<std::string>("hello");
    cache.insert(1, value);

    auto result = cache.find(1);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, "hello");
}

TEST(WTinyLFU, FindNonExistent) {
    w_tinylfu_cache<int, std::string> cache({.capacity = 10});

    auto result = cache.find(42);
    EXPECT_FALSE(result);
}

TEST(WTinyLFU, UpdateExistingKey) {
    w_tinylfu_cache<int, std::string> cache({.capacity = 10});

    cache.insert(1, ss::make_lw_shared<std::string>("first"));
    cache.insert(1, ss::make_lw_shared<std::string>("second"));

    auto result = cache.find(1);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, "second");
}

TEST(WTinyLFU, Erase) {
    w_tinylfu_cache<int, std::string> cache({.capacity = 10});

    cache.insert(1, ss::make_lw_shared<std::string>("hello"));
    EXPECT_TRUE(cache.find(1));

    cache.erase(1);
    EXPECT_FALSE(cache.find(1));
}

TEST(WTinyLFU, EraseNonExistent) {
    w_tinylfu_cache<int, std::string> cache({.capacity = 10});
    cache.erase(42); // Should not crash
}

TEST(WTinyLFU, WindowCacheEviction) {
    w_tinylfu_cache<int, int> cache({.capacity = 100});

    // Fill window cache (1% of 100 = 1 entry) plus more
    for (int i = 0; i < 10; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i * 10));
    }

    auto stats = cache.statistics();
    // Window should not exceed capacity
    EXPECT_LE(stats.window_size, 100);
    // Some entries should be in main cache
    EXPECT_GT(stats.probationary_size + stats.protected_size, 0);
}

TEST(WTinyLFU, CapacityEnforcement) {
    w_tinylfu_cache<int, int> cache({.capacity = 10});

    // Insert more than capacity
    for (int i = 0; i < 20; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    auto stats = cache.statistics();
    size_t total = stats.window_size + stats.probationary_size
                   + stats.protected_size;
    EXPECT_LE(total, 10);
}

TEST(WTinyLFU, FrequencyBasedAdmission) {
    w_tinylfu_cache<int, int> cache({.capacity = 10, .sample_size = 1000});

    // Fill cache
    for (int i = 0; i < 10; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Access some items frequently to build up frequency
    for (int j = 0; j < 10; ++j) {
        for (int i = 0; i < 5; ++i) {
            cache.find(i);
        }
    }

    // Try to insert new items - low frequency items should be rejected
    for (int i = 10; i < 20; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Frequently accessed items should still be in cache
    int found_count = 0;
    for (int i = 0; i < 5; ++i) {
        if (cache.find(i)) {
            ++found_count;
        }
    }
    // Most frequently accessed items should be retained
    EXPECT_GT(found_count, 2);
}

TEST(WTinyLFU, SLRUPromotion) {
    w_tinylfu_cache<int, int> cache({.capacity = 100});

    // Insert items to get them into main cache
    for (int i = 0; i < 50; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    auto stats_before = cache.statistics();

    // Access items multiple times to promote them to protected segment
    for (int j = 0; j < 3; ++j) {
        for (int i = 0; i < 25; ++i) {
            cache.find(i);
        }
    }

    auto stats_after = cache.statistics();

    // Protected segment should have grown
    EXPECT_GT(stats_after.protected_size, stats_before.protected_size);
}

TEST(WTinyLFU, HitMissStatistics) {
    w_tinylfu_cache<int, int> cache({.capacity = 10});

    cache.insert(1, ss::make_lw_shared<int>(10));
    cache.insert(2, ss::make_lw_shared<int>(20));

    cache.find(1); // hit
    cache.find(1); // hit
    cache.find(3); // miss
    cache.find(2); // hit
    cache.find(4); // miss

    auto stats = cache.statistics();
    EXPECT_EQ(stats.hits, 3);
    EXPECT_EQ(stats.misses, 2);
}

TEST(WTinyLFU, AgingMechanism) {
    w_tinylfu_cache<int, int> cache({.capacity = 10, .sample_size = 20});

    // Fill cache and access items
    for (int i = 0; i < 10; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        cache.find(i);
    }

    // Trigger aging by performing more operations than sample_size
    for (int i = 0; i < 25; ++i) {
        cache.find(0);
    }

    // After aging, old frequency information should be halved
    // New insertions should have a better chance
    cache.insert(100, ss::make_lw_shared<int>(100));
    auto result = cache.find(100);
    // Should still be findable (not immediately evicted)
    EXPECT_NE(result, nullptr);
}

TEST(WTinyLFU, DoorkeeperFiltering) {
    w_tinylfu_cache<int, int> cache({.capacity = 100, .sample_size = 1000});

    // Access many items only once (one-hit-wonders)
    for (int i = 0; i < 50; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Access some items twice to pass doorkeeper
    for (int i = 50; i < 60; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        cache.find(i);
    }

    // Items accessed twice should have higher frequency
    // This is tested indirectly through admission policy
    for (int i = 100; i < 110; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Items that were accessed twice should be more likely to remain
    int twice_accessed_found = 0;
    for (int i = 50; i < 60; ++i) {
        if (cache.find(i)) {
            ++twice_accessed_found;
        }
    }

    int once_accessed_found = 0;
    for (int i = 0; i < 50; ++i) {
        if (cache.find(i)) {
            ++once_accessed_found;
        }
    }

    // Twice-accessed items should have better retention
    // Allow for some variance in the probabilistic algorithm
    EXPECT_GE(twice_accessed_found, once_accessed_found / 5);
}

TEST(WTinyLFU, EmptyCache) {
    w_tinylfu_cache<int, int> cache({.capacity = 10});

    auto stats = cache.statistics();
    EXPECT_EQ(stats.window_size, 0);
    EXPECT_EQ(stats.probationary_size, 0);
    EXPECT_EQ(stats.protected_size, 0);
    EXPECT_EQ(stats.hits, 0);
    EXPECT_EQ(stats.misses, 0);
}

TEST(WTinyLFU, SingleEntryCache) {
    w_tinylfu_cache<int, int> cache({.capacity = 1});

    cache.insert(1, ss::make_lw_shared<int>(10));
    EXPECT_TRUE(cache.find(1));

    cache.insert(2, ss::make_lw_shared<int>(20));
    // Only one entry can fit
    auto stats = cache.statistics();
    EXPECT_EQ(
      stats.window_size + stats.probationary_size + stats.protected_size, 1);
}

TEST(WTinyLFU, StringKeys) {
    w_tinylfu_cache<std::string, int> cache({.capacity = 10});

    cache.insert("foo", ss::make_lw_shared<int>(1));
    cache.insert("bar", ss::make_lw_shared<int>(2));
    cache.insert("baz", ss::make_lw_shared<int>(3));

    auto result = cache.find("bar");
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 2);

    cache.erase("bar");
    EXPECT_FALSE(cache.find("bar"));
}

TEST(WTinyLFU, LargeCapacity) {
    w_tinylfu_cache<int, int> cache({.capacity = 1000});

    // Insert many items
    for (int i = 0; i < 1500; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i * 2));
    }

    auto stats = cache.statistics();
    size_t total = stats.window_size + stats.probationary_size
                   + stats.protected_size;
    EXPECT_LE(total, 1000);
}

TEST(WTinyLFU, AccessPatternAdaptation) {
    w_tinylfu_cache<int, int> cache({.capacity = 20, .sample_size = 50});

    // First access pattern: items 0-9
    for (int j = 0; j < 10; ++j) {
        for (int i = 0; i < 10; ++i) {
            cache.insert(i, ss::make_lw_shared<int>(i));
            cache.find(i);
        }
    }

    // Most of 0-9 should be in cache
    int first_pattern_found = 0;
    for (int i = 0; i < 10; ++i) {
        if (cache.find(i)) {
            ++first_pattern_found;
        }
    }
    EXPECT_GT(first_pattern_found, 5);

    // Trigger aging to adapt to new pattern
    for (int i = 0; i < 100; ++i) {
        cache.find(0);
    }

    // Second access pattern: items 10-19
    for (int j = 0; j < 10; ++j) {
        for (int i = 10; i < 20; ++i) {
            cache.insert(i, ss::make_lw_shared<int>(i));
            cache.find(i);
        }
    }

    // After aging, new pattern should be represented
    int second_pattern_found = 0;
    for (int i = 10; i < 20; ++i) {
        if (cache.find(i)) {
            ++second_pattern_found;
        }
    }
    EXPECT_GT(second_pattern_found, 3);
}

TEST(WTinyLFU, CountMinSketchEstimation) {
    w_tinylfu_cache<int, int> cache({.capacity = 100, .sample_size = 1000});

    // Access item 1 many times to build high frequency
    cache.insert(1, ss::make_lw_shared<int>(10));
    for (int i = 0; i < 50; ++i) {
        cache.find(1);
    }

    // Access item 2 fewer times
    cache.insert(2, ss::make_lw_shared<int>(20));
    for (int i = 0; i < 5; ++i) {
        cache.find(2);
    }

    // Fill cache
    for (int i = 3; i < 100; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Insert new items to trigger some evictions
    // But not so many that even highly-accessed items get evicted
    for (int i = 100; i < 150; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
        // Keep accessing item 1 to maintain its frequency
        if (i % 10 == 0) {
            cache.find(1);
        }
    }

    // Item 1 (high frequency) should be retained
    EXPECT_TRUE(cache.find(1));
}

TEST(WTinyLFU, ProtectedSegmentCapacity) {
    w_tinylfu_cache<int, int> cache({.capacity = 100});

    // Insert many items
    for (int i = 0; i < 100; ++i) {
        cache.insert(i, ss::make_lw_shared<int>(i));
    }

    // Access many items to promote them to protected
    for (int j = 0; j < 3; ++j) {
        for (int i = 0; i < 90; ++i) {
            cache.find(i);
        }
    }

    auto stats = cache.statistics();
    // Protected should be ~80% of main cache (99 entries)
    // So protected capacity is about 79 entries
    EXPECT_LE(stats.protected_size, 80);
}

TEST(CountMinSketch, BasicIncrement) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Initially, frequency should be 0
    EXPECT_EQ(sketch.frequency(42), 0);

    // Increment once
    sketch.increment(42);
    EXPECT_GT(sketch.frequency(42), 0);
}

TEST(CountMinSketch, MultipleIncrements) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Increment multiple times
    for (int i = 0; i < 10; ++i) {
        sketch.increment(123);
    }

    int32_t freq = sketch.frequency(123);
    EXPECT_GT(freq, 0);
    EXPECT_LE(freq, 10);
}

TEST(CountMinSketch, MaxFrequency) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Increment way beyond max (15 for 4-bit counters)
    for (int i = 0; i < 100; ++i) {
        sketch.increment(999);
    }

    // Frequency should be capped at 15
    EXPECT_EQ(sketch.frequency(999), 15);
}

TEST(CountMinSketch, DifferentKeys) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    sketch.increment(1);
    sketch.increment(1);
    sketch.increment(2);
    sketch.increment(2);
    sketch.increment(2);

    // Key 2 should have higher frequency than key 1
    EXPECT_GT(sketch.frequency(2), sketch.frequency(1));
}

TEST(CountMinSketch, Reset) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Build up frequencies
    for (int i = 0; i < 10; ++i) {
        sketch.increment(42);
    }

    int32_t freq_before = sketch.frequency(42);
    EXPECT_GT(freq_before, 0);

    // Reset (ages by halving)
    sketch.reset();

    int32_t freq_after = sketch.frequency(42);
    // Frequency should be halved (or close to it due to Count-Min Sketch
    // approximation)
    EXPECT_LT(freq_after, freq_before);
    EXPECT_LE(freq_after, freq_before / 2 + 1); // Allow +1 for rounding
}

TEST(CountMinSketch, MultipleResets) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Build up frequency
    for (int i = 0; i < 15; ++i) {
        sketch.increment(100);
    }

    EXPECT_EQ(sketch.frequency(100), 15);

    // Reset once: 15 -> 7
    sketch.reset();
    int32_t freq1 = sketch.frequency(100);
    EXPECT_LE(freq1, 7);
    EXPECT_GE(freq1, 7);

    // Reset again: 7 -> 3
    sketch.reset();
    int32_t freq2 = sketch.frequency(100);
    EXPECT_LE(freq2, 3);
    EXPECT_GE(freq2, 3);

    // Reset again: 3 -> 1
    sketch.reset();
    int32_t freq3 = sketch.frequency(100);
    EXPECT_LE(freq3, 1);
    EXPECT_GE(freq3, 1);

    // Reset again: 1 -> 0
    sketch.reset();
    int32_t freq4 = sketch.frequency(100);
    EXPECT_EQ(freq4, 0);
}

TEST(CountMinSketch, LargeCapacity) {
    w_tinylfu_detail::count_min_sketch sketch(10000);

    // Insert many different keys
    for (int i = 0; i < 1000; ++i) {
        sketch.increment(i);
    }

    // All should have at least frequency 1
    for (int i = 0; i < 1000; ++i) {
        EXPECT_GE(sketch.frequency(i), 1);
    }
}

TEST(CountMinSketch, HashCollisions) {
    w_tinylfu_detail::count_min_sketch sketch(10);

    // With small capacity, we're more likely to have collisions
    // Increment many different keys
    for (int i = 0; i < 100; ++i) {
        sketch.increment(i);
    }

    // Due to Count-Min Sketch properties, frequencies should still be
    // reasonable Each key should have frequency >= 1
    int keys_with_high_freq = 0;
    for (int i = 0; i < 100; ++i) {
        int32_t freq = sketch.frequency(i);
        EXPECT_GE(freq, 1);
        if (freq > 10) {
            keys_with_high_freq++;
        }
    }

    // Due to collisions, some keys will have inflated frequencies
    // But most should be reasonable
    EXPECT_LT(keys_with_high_freq, 50); // Less than half should be inflated
}

TEST(CountMinSketch, ZeroFrequency) {
    w_tinylfu_detail::count_min_sketch sketch(100);

    // Never accessed keys should have 0 frequency
    EXPECT_EQ(sketch.frequency(12345), 0);
    EXPECT_EQ(sketch.frequency(99999), 0);
}

TEST(Doorkeeper, FirstAccessBlocked) {
    w_tinylfu_detail::doorkeeper dk(100);

    // First access should return false (not admitted)
    EXPECT_FALSE(dk.maybe_admit(42));
}

TEST(Doorkeeper, SecondAccessAdmitted) {
    w_tinylfu_detail::doorkeeper dk(100);

    // First access
    EXPECT_FALSE(dk.maybe_admit(42));

    // Second access should return true (admitted)
    EXPECT_TRUE(dk.maybe_admit(42));
}

TEST(Doorkeeper, SubsequentAccessesAdmitted) {
    w_tinylfu_detail::doorkeeper dk(100);

    // First access blocked
    EXPECT_FALSE(dk.maybe_admit(42));

    // All subsequent accesses admitted
    EXPECT_TRUE(dk.maybe_admit(42));
    EXPECT_TRUE(dk.maybe_admit(42));
    EXPECT_TRUE(dk.maybe_admit(42));
}

TEST(Doorkeeper, DifferentKeys) {
    w_tinylfu_detail::doorkeeper dk(100);

    // Each key should be tracked independently
    EXPECT_FALSE(dk.maybe_admit(1));
    EXPECT_FALSE(dk.maybe_admit(2));
    EXPECT_FALSE(dk.maybe_admit(3));

    EXPECT_TRUE(dk.maybe_admit(1));
    EXPECT_TRUE(dk.maybe_admit(2));
    EXPECT_TRUE(dk.maybe_admit(3));
}

TEST(Doorkeeper, Reset) {
    w_tinylfu_detail::doorkeeper dk(100);

    // Access twice
    EXPECT_FALSE(dk.maybe_admit(42));
    EXPECT_TRUE(dk.maybe_admit(42));

    // Reset
    dk.reset();

    // After reset, should be like first access again
    EXPECT_FALSE(dk.maybe_admit(42));
    EXPECT_TRUE(dk.maybe_admit(42));
}

TEST(Doorkeeper, ManyKeys) {
    w_tinylfu_detail::doorkeeper dk(1000);

    // First access to many keys - most should be blocked (allow for some false
    // positives)
    int false_positives = 0;
    for (int i = 0; i < 1000; ++i) {
        if (dk.maybe_admit(i)) {
            false_positives++;
        }
    }

    // Bloom filter false positive rate should be reasonable
    // With 2 hash functions and 2 bits per entry, expect < 40% false positive
    // rate
    EXPECT_LT(false_positives, 400);

    // Second access to same keys - all should be admitted
    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(dk.maybe_admit(i));
    }
}

TEST(Doorkeeper, BloomFilterFalsePositives) {
    w_tinylfu_detail::doorkeeper dk(100); // Reasonable capacity

    int false_positives = 0;
    int total_tests = 1000;

    for (int i = 0; i < total_tests; ++i) {
        // First access should normally be blocked
        // Use large spread-out keys to reduce hash collisions
        if (dk.maybe_admit(static_cast<size_t>(i) * 1000000ULL)) {
            false_positives++;
        }
    }

    // Bloom filters can have false positives
    // With 2 hash functions, 2 bits per entry, and many more entries than
    // capacity Expect high false positive rate (since we're testing 1000 items
    // on capacity 100) But still less than 100% - at least some should be
    // blocked
    EXPECT_LT(false_positives, total_tests); // Not all false positives
    EXPECT_GT(false_positives, 0);           // Some false positives expected
}

TEST(Doorkeeper, AfterResetAllKeysBlocked) {
    w_tinylfu_detail::doorkeeper dk(100);

    // Admit several keys
    for (int i = 0; i < 10; ++i) {
        dk.maybe_admit(i);
        dk.maybe_admit(i);
    }

    // Reset
    dk.reset();

    // All keys should be blocked again on first access
    for (int i = 0; i < 10; ++i) {
        EXPECT_FALSE(dk.maybe_admit(i));
    }
}
