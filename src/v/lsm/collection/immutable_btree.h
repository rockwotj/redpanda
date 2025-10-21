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

#include "base/seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/generator.hh>

#include <compare>
#include <cstdint>
#include <optional>

namespace lsm::collection {

/// An immutable AVL tree implementation.
///
/// This tree is immutable, meaning that all operations (insert, remove)
/// return a new tree rather than modifying the existing tree. Nodes are
/// shared using seastar::lw_shared_ptr, so structural sharing is used
/// to avoid copying the entire tree on each operation.
///
/// The tree uses the three-way comparison operator (<=>) for comparing keys.
template<typename Key, typename Value>
requires std::three_way_comparable<Key>
class immutable_btree {
public:
    using generator
      = ss::coroutine::experimental::generator<std::pair<Key, Value>>;

    immutable_btree() = default;

    /// Insert a key-value pair into the tree.
    /// Returns a new tree with the key-value pair inserted.
    /// If the key already exists, the value is updated.
    [[nodiscard]] immutable_btree
    insert(const Key& key, const Value& value) const;

    /// Remove a key from the tree.
    /// Returns a new tree with the key removed.
    /// If the key does not exist, returns an unchanged tree.
    [[nodiscard]] immutable_btree remove(const Key& key) const;

    /// Get the value associated with a key.
    /// Returns std::nullopt if the key does not exist.
    [[nodiscard]] std::optional<Value> get(const Key& key) const;

    /// Returns true if the tree is empty.
    [[nodiscard]] bool empty() const { return _root == nullptr; }

    /// Returns the number of elements in the tree.
    [[nodiscard]] size_t size() const { return _root ? _root->size : 0; }

    /// Returns a generator over the in-order contents of the tree.
    [[nodiscard]] generator iterator() const {
        if (!_root) {
            co_return;
        }
        auto gen = iterate(_root);
        while (auto result = co_await gen()) {
            co_yield *result;
        }
    }

private:
    struct node {
        Key key;
        Value value;
        int32_t height;
        size_t size; // number of nodes in subtree
        ss::lw_shared_ptr<node> left;
        ss::lw_shared_ptr<node> right;

        node(Key k, Value v)
          : key(std::move(k))
          , value(std::move(v))
          , height(1)
          , size(1)
          , left(nullptr)
          , right(nullptr) {}

        node(
          Key k, Value v, ss::lw_shared_ptr<node> l, ss::lw_shared_ptr<node> r)
          : key(std::move(k))
          , value(std::move(v))
          , height(1 + std::max(get_height(l), get_height(r)))
          , size(1 + get_size(l) + get_size(r))
          , left(std::move(l))
          , right(std::move(r)) {}
    };

    static generator iterate(const ss::lw_shared_ptr<node>& n) {
        if (n->left) {
            auto gen = iterate(n->left);
            while (auto result = co_await gen()) {
                co_yield *result;
            }
        }
        co_yield std::make_pair(n->key, n->value);
        if (n->right) {
            auto gen = iterate(n->right);
            while (auto result = co_await gen()) {
                co_yield *result;
            }
        }
    }

    static int32_t get_height(const ss::lw_shared_ptr<node>& n) {
        return n ? n->height : 0;
    }

    static size_t get_size(const ss::lw_shared_ptr<node>& n) {
        return n ? n->size : 0;
    }

    static int32_t get_balance(const ss::lw_shared_ptr<node>& n) {
        return n ? get_height(n->left) - get_height(n->right) : 0;
    }

    static ss::lw_shared_ptr<node> rotate_right(ss::lw_shared_ptr<node> y) {
        auto x = y->left;
        auto T2 = x->right;

        // Perform rotation
        auto new_y = ss::make_lw_shared<node>(y->key, y->value, T2, y->right);
        auto new_x = ss::make_lw_shared<node>(x->key, x->value, x->left, new_y);

        return new_x;
    }

    static ss::lw_shared_ptr<node> rotate_left(ss::lw_shared_ptr<node> x) {
        auto y = x->right;
        auto T2 = y->left;

        // Perform rotation
        auto new_x = ss::make_lw_shared<node>(x->key, x->value, x->left, T2);
        auto new_y = ss::make_lw_shared<node>(
          y->key, y->value, new_x, y->right);

        return new_y;
    }

    static ss::lw_shared_ptr<node>
    insert_impl(ss::lw_shared_ptr<node> n, const Key& key, const Value& value) {
        // Base case: create new node
        if (!n) {
            return ss::make_lw_shared<node>(key, value);
        }

        // Compare and recurse
        auto cmp = key <=> n->key;
        ss::lw_shared_ptr<node> new_left = n->left;
        ss::lw_shared_ptr<node> new_right = n->right;
        Key new_key = n->key;
        Value new_value = n->value;

        if (cmp < 0) {
            new_left = insert_impl(n->left, key, value);
        } else if (cmp > 0) {
            new_right = insert_impl(n->right, key, value);
        } else {
            // Key already exists, update value
            new_key = key;
            new_value = value;
        }

        // Create new node with updated children
        auto new_node = ss::make_lw_shared<node>(
          std::move(new_key), std::move(new_value), new_left, new_right);

        // Get balance factor
        int32_t balance = get_balance(new_node);

        // Left Left Case
        if (balance > 1 && key < new_node->left->key) {
            return rotate_right(new_node);
        }

        // Right Right Case
        if (balance < -1 && key > new_node->right->key) {
            return rotate_left(new_node);
        }

        // Left Right Case
        if (balance > 1 && key > new_node->left->key) {
            new_left = rotate_left(new_node->left);
            new_node = ss::make_lw_shared<node>(
              new_node->key, new_node->value, new_left, new_node->right);
            return rotate_right(new_node);
        }

        // Right Left Case
        if (balance < -1 && key < new_node->right->key) {
            new_right = rotate_right(new_node->right);
            new_node = ss::make_lw_shared<node>(
              new_node->key, new_node->value, new_node->left, new_right);
            return rotate_left(new_node);
        }

        return new_node;
    }

    static ss::lw_shared_ptr<node> find_min(ss::lw_shared_ptr<node> n) {
        while (n->left) {
            n = n->left;
        }
        return n;
    }

    static ss::lw_shared_ptr<node>
    remove_impl(ss::lw_shared_ptr<node> n, const Key& key) {
        if (!n) {
            return nullptr;
        }

        auto cmp = key <=> n->key;
        ss::lw_shared_ptr<node> new_left = n->left;
        ss::lw_shared_ptr<node> new_right = n->right;

        if (cmp < 0) {
            new_left = remove_impl(n->left, key);
            if (new_left == n->left && n->right == new_right) {
                return n; // No change
            }
        } else if (cmp > 0) {
            new_right = remove_impl(n->right, key);
            if (new_left == n->left && n->right == new_right) {
                return n; // No change
            }
        } else {
            // Node to be deleted found
            if (!n->left || !n->right) {
                // Node with only one child or no child
                return n->left ? n->left : n->right;
            }

            // Node with two children: get inorder successor
            auto min_node = find_min(n->right);
            new_right = remove_impl(n->right, min_node->key);

            // Create new node with successor's key/value
            auto new_node = ss::make_lw_shared<node>(
              min_node->key, min_node->value, n->left, new_right);

            // Balance the tree
            int32_t balance = get_balance(new_node);

            // Left Left Case
            if (balance > 1 && get_balance(new_node->left) >= 0) {
                return rotate_right(new_node);
            }

            // Left Right Case
            if (balance > 1 && get_balance(new_node->left) < 0) {
                new_left = rotate_left(new_node->left);
                new_node = ss::make_lw_shared<node>(
                  new_node->key, new_node->value, new_left, new_node->right);
                return rotate_right(new_node);
            }

            // Right Right Case
            if (balance < -1 && get_balance(new_node->right) <= 0) {
                return rotate_left(new_node);
            }

            // Right Left Case
            if (balance < -1 && get_balance(new_node->right) > 0) {
                new_right = rotate_right(new_node->right);
                new_node = ss::make_lw_shared<node>(
                  new_node->key, new_node->value, new_node->left, new_right);
                return rotate_left(new_node);
            }

            return new_node;
        }

        // Create new node with potentially updated children
        auto new_node = ss::make_lw_shared<node>(
          n->key, n->value, new_left, new_right);

        // Balance the tree
        int32_t balance = get_balance(new_node);

        // Left Left Case
        if (balance > 1 && get_balance(new_node->left) >= 0) {
            return rotate_right(new_node);
        }

        // Left Right Case
        if (balance > 1 && get_balance(new_node->left) < 0) {
            new_left = rotate_left(new_node->left);
            new_node = ss::make_lw_shared<node>(
              new_node->key, new_node->value, new_left, new_node->right);
            return rotate_right(new_node);
        }

        // Right Right Case
        if (balance < -1 && get_balance(new_node->right) <= 0) {
            return rotate_left(new_node);
        }

        // Right Left Case
        if (balance < -1 && get_balance(new_node->right) > 0) {
            new_right = rotate_right(new_node->right);
            new_node = ss::make_lw_shared<node>(
              new_node->key, new_node->value, new_node->left, new_right);
            return rotate_left(new_node);
        }

        return new_node;
    }

    explicit immutable_btree(ss::lw_shared_ptr<node> root)
      : _root(std::move(root)) {}

    ss::lw_shared_ptr<node> _root;
};

template<typename Key, typename Value>
requires std::three_way_comparable<Key>
immutable_btree<Key, Value>
immutable_btree<Key, Value>::insert(const Key& key, const Value& value) const {
    return immutable_btree(insert_impl(_root, key, value));
}

template<typename Key, typename Value>
requires std::three_way_comparable<Key>
immutable_btree<Key, Value>
immutable_btree<Key, Value>::remove(const Key& key) const {
    return immutable_btree(remove_impl(_root, key));
}

template<typename Key, typename Value>
requires std::three_way_comparable<Key>
std::optional<Value> immutable_btree<Key, Value>::get(const Key& key) const {
    auto current = _root;
    while (current) {
        auto cmp = key <=> current->key;
        if (cmp < 0) {
            current = current->left;
        } else if (cmp > 0) {
            current = current->right;
        } else {
            return current->value;
        }
    }
    return std::nullopt;
}

} // namespace lsm::collection
