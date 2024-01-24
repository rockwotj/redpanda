/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <compare>
#include <cstddef>
#include <iterator>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

/**
 * A very very simple fragmented vector that provides random access like a
 * vector, but does not store its data in contiguous memory.
 *
 * There is no reserve method because we allocate a full fragment at a time.
 * However, after you populate a vector you might want to call shrink to fit if
 * your fragment is large. A more advanced strategy could allocate capacity up
 * front which just requires a bit more accounting.
 *
 * The iterator implementation works for a few things like std::lower_bound,
 * upper_bound, distance, etc... see fragmented_vector_test.
 *
 * Note that the decision to allocate a full fragment at a time isn't
 * necessarily an optimization, but rather a restriction that simplifies the
 * implementation. If expand fragmented_vector to be more general purpose, we
 * should indeed make it more flexible. If we add a reserve method to allocate
 * all of the space at once we might benefit from the allocator helping with
 * giving us contiguous memory.
 *
 * If fragment_size_bytes is equal to std::dynamic_extent then we will
 * switch out the allocation strategy to be the standard vector capacity
 * doubling for the first chunk, but then subsequent chunks will be allocated at
 * the full max allocation size we recommend.
 */
template<typename T, size_t fragment_size_bytes = 8192>
class fragmented_vector {
    // calculate the maximum number of elements per fragment while
    // keeping the element count a power of two
    static constexpr size_t calc_elems_per_frag(size_t esize) {
        size_t max = fragment_size_bytes / esize;
        if constexpr (fragment_size_bytes == std::dynamic_extent) {
            constexpr size_t max_allocation_size = 128UL * 1024;
            max = max_allocation_size / esize;
        }
        assert(max > 0);
        // round down to a power of two
        size_t pow2 = 1;
        while (pow2 * 2 <= max) {
            pow2 *= 2;
        }
        return pow2;
    }

    static constexpr size_t elems_per_frag = calc_elems_per_frag(sizeof(T));

    static_assert(
      (elems_per_frag & (elems_per_frag - 1)) == 0,
      "element count per fragment must be a power of 2");
    static_assert(elems_per_frag >= 1);

public:
    using this_type = fragmented_vector<T, fragment_size_bytes>;
    using value_type = T;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;

    /**
     * The maximum number of bytes per fragment as specified in
     * as part of the type. Note that for most types, the true
     * number of bytes in a full fragment may as low as half
     * of this amount (+1) since the number of elements is restricted
     * to a power of two.
     */
    static constexpr size_t max_frag_bytes = fragment_size_bytes;

    fragmented_vector() noexcept = default;
    fragmented_vector& operator=(const fragmented_vector&) noexcept = delete;
    fragmented_vector(fragmented_vector&& other) noexcept {
        *this = std::move(other);
    }
    fragmented_vector& operator=(fragmented_vector&& other) noexcept {
        if (this != &other) {
            this->_size = other._size;
            this->_capacity = other._capacity;
            this->_frags = std::move(other._frags);
            // Move compatibility with std::vector that post move
            // the vector is empty().
            other._size = other._capacity = 0;
            other.update_generation();
            update_generation();
        }
        return *this;
    }
    ~fragmented_vector() noexcept = default;

    template<typename Iter>
    requires std::input_iterator<Iter>
    fragmented_vector(Iter begin, Iter end)
      : fragmented_vector() {
        // Improvement: Write a more efficient implementation for
        // random_access_iterators
        for (auto it = begin; it != end; ++it) {
            push_back(*it);
        }
    }

    fragmented_vector copy() const noexcept { return *this; }

    void swap(fragmented_vector& other) noexcept {
        std::swap(_size, other._size);
        std::swap(_capacity, other._capacity);
        std::swap(_frags, other._frags);
        other.update_generation();
        update_generation();
    }

    template<class E = T>
    void push_back(E&& elem) {
        maybe_add_capacity();
        _frags.back().push_back(std::forward<E>(elem));
        ++_size;
        update_generation();
    }

    template<class... Args>
    T& emplace_back(Args&&... args) {
        maybe_add_capacity();
        _frags.back().emplace_back(std::forward<Args>(args)...);
        ++_size;
        update_generation();
        return _frags.back().back();
    }

    void pop_back() {
        vassert(_size > 0, "Cannot pop from empty container");
        _frags.back().pop_back();
        --_size;
        if (_frags.back().empty()) {
            _frags.pop_back();
            _capacity -= elems_per_frag;
        }
        update_generation();
    }

    /*
     * Replacement for `erase(some_it, end())` but more efficient than n
     * `pop_back()s`
     */
    void pop_back_n(size_t n) {
        vassert(
          _size >= n, "Cannot pop more than size() elements in container");

        if (_size == n) {
            clear();
            return;
        }

        _size -= n;

        while (n >= _frags.back().size()) {
            n -= _frags.back().size();
            _frags.pop_back();
            _capacity -= elems_per_frag;
        }

        for (size_t i = 0; i < n; ++i) {
            _frags.back().pop_back();
        }
        update_generation();
    }

    const T& at(size_t index) const {
        return _frags.at(index / elems_per_frag).at(index % elems_per_frag);
    }

    T& at(size_t index) {
        return _frags.at(index / elems_per_frag).at(index % elems_per_frag);
    }

    const T& operator[](size_t index) const {
        return _frags[index / elems_per_frag][index % elems_per_frag];
    }

    T& operator[](size_t index) {
        return _frags[index / elems_per_frag][index % elems_per_frag];
    }

    const T& front() const { return _frags.front().front(); }
    const T& back() const { return _frags.back().back(); }
    T& front() { return _frags.front().front(); }
    T& back() { return _frags.back().back(); }
    bool empty() const noexcept { return _size == 0; }
    size_t size() const noexcept { return _size; }

    void shrink_to_fit() {
        // if (!_frags.empty()) {
        //    _frags.back().shrink_to_fit();
        // }
    }

    void reserve(size_t v) {
        if constexpr (fragment_size_bytes == std::dynamic_extent) {
            _frags.front().reserve(std::min(v, elems_per_frag));
        }
        update_generation();
    }

    bool operator==(const fragmented_vector& o) const noexcept {
        return o._frags == _frags;
    }

    /**
     * Returns the approximate in-memory size of this vector in bytes.
     */
    size_t memory_size() const {
        return _frags.size() * (sizeof(_frags[0]) + elems_per_frag * sizeof(T));
    }

    /**
     * Returns the (maximum) number of elements in each fragment of this vector.
     */
    static constexpr size_t elements_per_fragment() { return elems_per_frag; }

    /**
     * Remove all elements from the vector.
     *
     * Unlike std::vector, this also releases all the memory from
     * the vector (since this vector already the same pointer
     * and iterator stability guarantees that std::vector provides
     * based on non-reallocation and capacity()).
     */
    void clear() {
        // do the swap dance to actually clear the memory held by the vector
        std::vector<std::vector<T>>{}.swap(_frags);
        _size = 0;
        _capacity = 0;
        update_generation();
    }

    template<bool C>
    class iter {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = typename std::conditional_t<C, const T, T>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        iter() = default;

        reference operator*() const {
            check_generation();
            return _vec->operator[](_index);
        }
        pointer operator->() const {
            check_generation();
            return &_vec->operator[](_index);
        }

        iter& operator+=(ssize_t n) {
            check_generation();
            _index += n;
            return *this;
        }

        iter& operator-=(ssize_t n) {
            check_generation();
            _index -= n;
            return *this;
        }

        iter& operator++() {
            check_generation();
            ++_index;
            return *this;
        }

        iter& operator--() {
            check_generation();
            --_index;
            return *this;
        }

        iter operator++(int) {
            check_generation();
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        iter operator--(int) {
            check_generation();
            auto tmp = *this;
            --*this;
            return tmp;
        }

        iter operator+(difference_type offset) {
            check_generation();
            return iter{*this} += offset;
        }
        iter operator-(difference_type offset) {
            check_generation();
            return iter{*this} -= offset;
        }

        bool operator==(const iter& o) const {
            check_generation();
            return _index == o._index && _vec == o._vec;
        };
        auto operator<=>(const iter& o) const {
            check_generation();
            auto cmp = _index <=> o._index;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
            return _vec <=> o._vec;
        };

        friend ssize_t operator-(const iter& a, const iter& b) {
            return a._index - b._index;
        }

    private:
        friend class fragmented_vector;
        using vec_type = std::conditional_t<C, const this_type, this_type>;

        iter(vec_type* vec, size_t index)
          : _index(index)
          , _vec(vec) {
#ifndef NDEBUG
            // NOLINTNEXTLINE(cppcoreguidelines-prefer-member-initializer)
            _my_generation = vec->_generation;
#endif
        }

        inline void check_generation() const {
#ifndef NDEBUG
            vassert(
              _vec->_generation == _my_generation,
              "Attempting to use an invalidated iterator. The corresponding "
              "fragmented_vector container has been mutated since this "
              "iterator was constructed.");
#endif
        }

        size_t _index{};
        vec_type* _vec{};
#ifndef NDEBUG
        size_t _my_generation{};
#endif
    };

    using const_iterator = iter<true>;
    using iterator = iter<false>;

    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, _size); }

    const_iterator begin() const { return const_iterator(this, 0); }
    const_iterator end() const { return const_iterator(this, _size); }

    const_iterator cbegin() const { return const_iterator(this, 0); }
    const_iterator cend() const { return const_iterator(this, _size); }

    friend std::ostream&
    operator<<(std::ostream& os, const fragmented_vector& v) {
        os << "[";
        for (auto& e : v) {
            os << e << ",";
        }
        os << "]";
        return os;
    }

private:
    void maybe_add_capacity() {
        if (_size == _capacity) {
            std::vector<T> frag;
            if (fragment_size_bytes != std::dynamic_extent || !_frags.empty()) {
                frag.reserve(elems_per_frag);
            }
            _frags.push_back(std::move(frag));
            _capacity += elems_per_frag;
        }
    }

    inline void update_generation() {
#ifndef NDEBUG
        ++_generation;
#endif
    }

private:
    friend class fragmented_vector_validator;
    fragmented_vector(const fragmented_vector&) noexcept = default;

    template<typename TT, size_t SS>
    friend seastar::future<>
    fragmented_vector_fill_async(fragmented_vector<TT, SS>&, const TT&);

    template<typename TT, size_t SS>
    friend seastar::future<>
    fragmented_vector_clear_async(fragmented_vector<TT, SS>&);

    size_t _size{0};
    size_t _capacity{0};
    std::vector<std::vector<T>> _frags;
#ifndef NDEBUG
    // Add a generation number that is incremented on every mutation to catch
    // invalidated iterator accesses.
    size_t _generation{0};
#endif
};

/**
 * An alias for a fragmented_vector using a larger fragment size, close
 * to the limit of the maximum contiguous allocation size.
 */
template<typename T>
using large_fragment_vector = fragmented_vector<T, 32 * 1024>;

/**
 * An alias for a fragmented_vector using a smaller fragment size.
 */
template<typename T>
using small_fragment_vector = fragmented_vector<T, 1024>;

/**
 * A vector that does not allocate large contiguous chunks. Instead the
 * allocations are broken up across many different individual vectors, but the
 * exposed view is of a single container.
 *
 * Additionally the allocation strategy is like a "normal" vector up to our max
 * recommended allocation size, at which we will then only allocate new chunks
 * and previous chunk elements will not be moved.
 */
template<typename T>
using chunked_vector = fragmented_vector<T, std::dynamic_extent>;

/**
 * A futurized version of std::fill optimized for fragmented vector. It is
 * futurized to allow for large vectors to be filled without incurring reactor
 * stalls. It is optimized by circumventing the indexing indirection incurred by
 * using the fragmented vector interface directly.
 */
template<typename T, size_t S>
inline seastar::future<>
fragmented_vector_fill_async(fragmented_vector<T, S>& vec, const T& value) {
    auto remaining = vec._size;
    for (auto& frag : vec._frags) {
        const auto n = std::min(frag.size(), remaining);
        if (n == 0) {
            break;
        }
        std::fill_n(frag.begin(), n, value);
        remaining -= n;
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vassert(
      remaining == 0,
      "fragmented vector inconsistency filling remaining {} size {} cap {} "
      "nfrags {}",
      remaining,
      vec._size,
      vec._capacity,
      vec._frags.size());
}

/**
 * A futurized version of fragmented_vector::clear that allows clearing a large
 * vector without incurring a reactor stall.
 */
template<typename T, size_t S>
inline seastar::future<>
fragmented_vector_clear_async(fragmented_vector<T, S>& vec) {
    while (!vec._frags.empty()) {
        vec._frags.pop_back();
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vec.clear();
}
