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
#pragma once

#include "absl/hash/hash.h"
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/shared_ptr.hh>

#include <boost/intrusive/list.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

/**
 * Window-TinyLFU Cache
 *
 * An implementation of the Window TinyLFU cache eviction algorithm.
 *
 * Einziger, et al., "TinyLFU: A Highly Efficient Cache Admission Policy",
 * ACM Transactions on Storage, Vol. 13, No. 4, 2017
 * https://dl.acm.org/doi/10.1145/3149371
 *
 * W-TinyLFU is a modern cache eviction algorithm that combines:
 * - A small window cache (1% capacity) using LRU for new items
 * - A main cache (99% capacity) using Segmented LRU (SLRU)
 * - An admission policy based on frequency estimation via Count-Min Sketch
 * - A doorkeeper filter to ignore one-hit-wonders
 *
 * The algorithm adapts to changing access patterns through periodic aging
 * of frequency counters, providing excellent hit rates across diverse
 * workloads.
 *
 * Usage
 * =====
 *
 * Create a cache with a specified capacity:
 *
 *     w_tinylfu_cache<int, std::string> cache({.capacity = 100});
 *
 * Insert items:
 *
 *     auto value = ss::make_lw_shared<std::string>("hello");
 *     cache.insert(42, value);
 *
 * Lookup items:
 *
 *     auto result = cache.find(42);
 *     if (result) {
 *         // Use *result
 *     }
 *
 * Remove items:
 *
 *     cache.erase(42);
 *
 * Get statistics:
 *
 *     auto stats = cache.statistics();
 *     fmt::print("Hit rate: {}\n",
 *         static_cast<double>(stats.hits) / (stats.hits + stats.misses));
 */

namespace lsm::utils {

namespace w_tinylfu_detail {

/**
 * Frequency Sketch for estimating item popularity.
 *
 * Based on Caffeine's FrequencySketch implementation, this is a 4-bit
 * Count-Min Sketch that packs 16 counters per 64-bit word for efficiency.
 * The maximum frequency is 15 (4 bits), and an aging process periodically
 * halves all counters to adapt to changing access patterns.
 *
 * Implementation ported from:
 * https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/FrequencySketch.java
 */
class count_min_sketch final {
public:
    /**
     * @brief Initializes or increases the capacity of the sketch.
     *
     * This ensures the sketch can accurately estimate popularity given the
     * maximum size of the cache. This operation discards all previous counts.
     *
     * @param maximumSize The maximum number of entries in the cache.
     */
    explicit count_min_sketch(int64_t maximum_size) {
        vassert(
          maximum_size > 0, "maximum_size must be positive: {}", maximum_size);
        int32_t maximum = static_cast<int32_t>(std::min(
          maximum_size,
          static_cast<int64_t>(std::numeric_limits<int32_t>::max()) >> 1u));
        if (!table_.empty() && table_.size() >= static_cast<size_t>(maximum)) {
            return;
        }
        uint32_t table_size = std::bit_ceil(static_cast<uint32_t>(maximum));
        table_.assign(table_size, 0);
        table_mask_ = static_cast<int>(table_.size() - 1);
    }

    /**
     * @brief Estimates the number of occurrences of an element.
     * @param e The element to count.
     * @return The estimated frequency, capped at 15.
     */
    int32_t frequency(uint64_t element_hash) const {
        int32_t hash = spread(compute_hash(element_hash));
        int32_t start = (hash & 3u) << 2u;
        int32_t frequency = std::numeric_limits<int32_t>::max();
        for (int i = 0; i < 4; ++i) {
            int index = index_of(hash, i);
            int count = static_cast<int32_t>(
              (table_[index] >> ((start + i) << 2)) & 0xFULL);
            frequency = std::min(frequency, count);
        }
        return frequency;
    }

    /**
     * @brief Increments the frequency count of an element.
     *
     * If the count is already at the maximum (15), it remains unchanged.
     * The sketch is periodically aged by halving all counts when a sample
     * size of operations is reached.
     *
     * @param e The element to increment.
     */
    void increment(uint64_t element_hash) {
        int32_t hash = spread(compute_hash(element_hash));
        int start = (hash & 3) << 2;

        int index0 = index_of(hash, 0);
        int index1 = index_of(hash, 1);
        int index2 = index_of(hash, 2);
        int index3 = index_of(hash, 3);
        increment_at(index0, start);
        increment_at(index1, start + 1);
        increment_at(index2, start + 2);
        increment_at(index3, start + 3);
    }

    /**
     * @brief Reduces every counter by half, aging the sketch.
     */
    void reset() {
        for (int64_t& entry : table_) {
            entry = (entry >> 1u) & reset_mask;
        }
    }

private:
    // A mixture of seeds from FNV-1a, CityHash, and Murmur3
    static constexpr std::array<uint64_t, 4> seed = {
      0xc3a5c85c97cb3127ULL,
      0xb492b66fbe98f273ULL,
      0x9ae16a3b2f90404fULL,
      0xcbf29ce484222325ULL};
    static constexpr uint64_t reset_mask = 0x7777777777777777ULL;
    static constexpr uint64_t one_mask = 0x1111111111111111ULL;

    int table_mask_{0};
    std::vector<int64_t> table_;

    /**
     * @brief Applies a supplemental hash function to defend against poor hash
     * functions.
     *
     * This is the MurmurHash3 finalizer. It uses unsigned arithmetic to
     * ensure defined wrap-around behavior, matching Java's integer overflow.
     */
    [[nodiscard]] static int32_t spread(int32_t x) {
        uint32_t h = static_cast<uint32_t>(x);
        h = ((h >> 16) ^ h) * 0x45d9f3b;
        h = ((h >> 16) ^ h) * 0x45d9f3b;
        return static_cast<int32_t>((h >> 16) ^ h);
    }

    /**
     * @brief Hashes an element to a 32-bit integer.
     */
    [[nodiscard]] static int32_t compute_hash(uint64_t element_hash) {
        // Mix 64-bit hash down to 32 bits to match Java's int hashCode
        return static_cast<int32_t>(element_hash ^ (element_hash >> 32));
    }

    /**
     * @brief Returns the table index for a given hash and depth.
     */
    [[nodiscard]] int index_of(int32_t item, int i) const {
        uint64_t hash = (static_cast<uint64_t>(item) + seed[i]) * seed[i];
        hash += (hash >> 32);
        return static_cast<int32_t>(hash) & table_mask_;
    }

    /**
     * @brief Increments the counter at a specific index if it's below 15.
     * @return True if the counter was incremented, false otherwise.
     */
    void increment_at(int i, int j) {
        int offset = j << 2;
        uint64_t mask = 0xFULL << offset;
        if ((table_[i] & mask) != mask) {
            table_[i] += (1ULL << offset);
        }
    }
};

/**
 * Doorkeeper Bloom Filter.
 *
 * Filters out items that are accessed only once, preventing them from
 * polluting the frequency sketch. Items must pass through the doorkeeper
 * (be seen twice) before their frequency is tracked.
 */
class doorkeeper {
public:
    explicit doorkeeper(size_t capacity)
      : bits_per_entry_(2)
      , num_bits_(capacity * bits_per_entry_)
      , num_hashes_(2)
      , bits_((num_bits_ + 63) / 64, 0) {}

    /**
     * Check if an item should be admitted to the frequency sketch.
     * Returns true if this is at least the second access.
     */
    bool maybe_admit(size_t hash) {
        bool all_set = true;
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t bit_pos = hash_to_bit(hash, i);
            size_t word_idx = bit_pos / 64;
            size_t bit_idx = bit_pos % 64;

            bool is_set = (bits_[word_idx] & (1ULL << bit_idx)) != 0;
            if (!is_set) {
                bits_[word_idx] |= (1ULL << bit_idx);
                all_set = false;
            }
        }
        return all_set;
    }

    /**
     * Clear the doorkeeper (called during aging).
     */
    void reset() { bits_.assign(bits_.size(), 0); }

private:
    size_t hash_to_bit(size_t hash, size_t salt) const {
        size_t h = hash ^ (salt * 0x517cc1b727220a95ULL);
        h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
        h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
        return (h ^ (h >> 31)) % num_bits_;
    }

    size_t bits_per_entry_;
    size_t num_bits_;
    size_t num_hashes_;
    std::vector<uint64_t> bits_;
};

} // namespace w_tinylfu_detail

/**
 * W-TinyLFU cache implementation.
 *
 * Template parameters:
 * - Key: The key type (must be hashable)
 * - Value: The value type (stored in ss::lw_shared_ptr<Value>)
 */
template<typename Key, typename Value>
class w_tinylfu_cache {
public:
    /**
     * Cache configuration.
     */
    struct config {
        /// Total cache capacity (number of entries)
        size_t capacity;
        /// Sample size for aging (default: 10 * capacity)
        size_t sample_size = 0;
    };

    /**
     * Cache statistics.
     */
    struct stats {
        size_t window_size;
        size_t probationary_size;
        size_t protected_size;
        size_t hits;
        size_t misses;
    };

    /**
     * Construct a W-TinyLFU cache with the given configuration.
     */
    explicit w_tinylfu_cache(config cfg)
      : capacity_(cfg.capacity)
      , window_capacity_(std::max<size_t>(1, capacity_ / 100))
      , main_capacity_(capacity_ - window_capacity_)
      , protected_capacity_((main_capacity_ * 80) / 100)
      , probationary_capacity_(main_capacity_ - protected_capacity_)
      , sample_size_(cfg.sample_size > 0 ? cfg.sample_size : 10 * cfg.capacity)
      , sketch_(cfg.capacity)
      , doorkeeper_(cfg.capacity) {}

    /**
     * Insert or update an entry in the cache.
     *
     * If the cache is full, items may be evicted according to the
     * W-TinyLFU policy.
     */
    void insert(const Key& key, ss::lw_shared_ptr<Value> value) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            // Update existing entry
            it->second->value = std::move(value);
            touch(*it->second);
            return;
        }

        // Make space if needed
        while (size() >= capacity_) {
            evict();
        }

        // New entry always goes to window cache
        auto entry = std::make_unique<cache_entry>();
        entry->key = key;
        entry->value = std::move(value);
        entry->loc = location::window;

        window_queue_.push_back(*entry);
        map_.emplace(key, std::move(entry));
        record_access(key);

        // Evict from window if it exceeds capacity
        while (window_queue_.size() > window_capacity_) {
            evict_from_window();
        }
    }

    /**
     * Find an entry in the cache.
     *
     * Returns an empty lw_shared_ptr if the key is not found.
     */
    ss::lw_shared_ptr<Value> find(const Key& key) {
        auto it = map_.find(key);
        if (it == map_.end()) {
            ++misses_;
            return {};
        }

        ++hits_;
        record_access(key);
        touch(*it->second);
        return it->second->value;
    }

    /**
     * Remove an entry from the cache.
     */
    void erase(const Key& key) {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return;
        }

        remove_from_queue(*it->second);
        map_.erase(it);
    }

    /**
     * Get current cache statistics.
     */
    stats statistics() const {
        return {
          .window_size = window_queue_.size(),
          .probationary_size = probationary_queue_.size(),
          .protected_size = protected_queue_.size(),
          .hits = hits_,
          .misses = misses_,
        };
    }

private:
    enum class location : uint8_t { window, probationary, protected_seg };

    struct cache_entry {
        Key key;
        ss::lw_shared_ptr<Value> value;
        location loc;
        boost::intrusive::list_member_hook<> hook;
    };

    using list_t = boost::intrusive::list<
      cache_entry,
      boost::intrusive::member_hook<
        cache_entry,
        boost::intrusive::list_member_hook<>,
        &cache_entry::hook>>;

    size_t size() const {
        return window_queue_.size() + probationary_queue_.size()
               + protected_queue_.size();
    }

    void record_access(const Key& key) {
        size_t hash = absl::Hash<Key>{}(key);

        // Increment operation count for aging
        if (++operation_count_ >= sample_size_) {
            operation_count_ = 0;
            sketch_.reset();
            doorkeeper_.reset();
        }

        // Only record in sketch if doorkeeper admits
        if (doorkeeper_.maybe_admit(hash)) {
            sketch_.increment(hash);
        }
    }

    void touch(cache_entry& entry) {
        switch (entry.loc) {
        case location::window:
            // Move to back of window queue (most recently used)
            window_queue_.erase(window_queue_.iterator_to(entry));
            window_queue_.push_back(entry);
            break;

        case location::probationary:
            // Promote to protected segment
            probationary_queue_.erase(probationary_queue_.iterator_to(entry));

            // Make space in protected if needed
            while (protected_queue_.size() >= protected_capacity_) {
                demote_from_protected();
            }

            protected_queue_.push_back(entry);
            entry.loc = location::protected_seg;
            break;

        case location::protected_seg:
            // Move to back of protected queue (most recently used)
            protected_queue_.erase(protected_queue_.iterator_to(entry));
            protected_queue_.push_back(entry);
            break;
        }
    }

    void remove_from_queue(cache_entry& entry) {
        switch (entry.loc) {
        case location::window:
            window_queue_.erase(window_queue_.iterator_to(entry));
            break;
        case location::probationary:
            probationary_queue_.erase(probationary_queue_.iterator_to(entry));
            break;
        case location::protected_seg:
            protected_queue_.erase(protected_queue_.iterator_to(entry));
            break;
        }
    }

    void demote_from_protected() {
        if (protected_queue_.empty()) {
            return;
        }

        // Evict from front of protected (least recently used)
        auto& entry = protected_queue_.front();
        protected_queue_.pop_front();
        probationary_queue_.push_back(entry);
        entry.loc = location::probationary;
    }

    void evict() {
        // Try to evict from window first if it's over capacity
        if (window_queue_.size() > window_capacity_) {
            evict_from_window();
        } else if (!probationary_queue_.empty()) {
            evict_from_probationary();
        } else if (!window_queue_.empty()) {
            evict_from_window();
        } else if (!protected_queue_.empty()) {
            // Last resort - evict from protected
            auto& entry = protected_queue_.front();
            protected_queue_.pop_front();
            map_.erase(entry.key);
        }
    }

    void evict_from_window() {
        if (window_queue_.empty()) {
            return;
        }

        auto& victim = window_queue_.front();
        window_queue_.pop_front();

        // Try to admit to main cache
        size_t candidate_freq = sketch_.frequency(
          absl::Hash<Key>{}(victim.key));

        // Check if we can admit to main cache without evicting
        size_t main_size = probationary_queue_.size() + protected_queue_.size();
        if (main_size < main_capacity_) {
            // Space available in main cache, admit directly
            probationary_queue_.push_back(victim);
            victim.loc = location::probationary;
            return;
        }

        // Main cache is full, need to compare frequencies
        if (!probationary_queue_.empty()) {
            auto& prob_victim = probationary_queue_.front();
            size_t victim_freq = sketch_.frequency(
              absl::Hash<Key>{}(prob_victim.key));

            if (candidate_freq > victim_freq) {
                // Admit window victim to main cache
                auto prob_victim_key = prob_victim.key;
                probationary_queue_.pop_front();
                map_.erase(prob_victim_key);

                probationary_queue_.push_back(victim);
                victim.loc = location::probationary;
            } else {
                // Reject window victim
                auto victim_key = victim.key;
                map_.erase(victim_key);
            }
        } else {
            // No probationary entries, admit to main cache
            probationary_queue_.push_back(victim);
            victim.loc = location::probationary;
        }
    }

    void evict_from_probationary() {
        if (probationary_queue_.empty()) {
            return;
        }

        auto& entry = probationary_queue_.front();
        auto key = entry.key;
        probationary_queue_.pop_front();
        map_.erase(key);
    }

    size_t capacity_;
    size_t window_capacity_;
    size_t main_capacity_;
    size_t protected_capacity_;
    size_t probationary_capacity_;
    size_t sample_size_;

    chunked_hash_map<Key, std::unique_ptr<cache_entry>> map_;
    list_t window_queue_;
    list_t probationary_queue_;
    list_t protected_queue_;

    w_tinylfu_detail::count_min_sketch sketch_;
    w_tinylfu_detail::doorkeeper doorkeeper_;

    size_t operation_count_ = 0;
    size_t hits_ = 0;
    size_t misses_ = 0;
};

} // namespace lsm::utils
