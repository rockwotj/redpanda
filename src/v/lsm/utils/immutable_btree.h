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

#include "base/seastarx.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/generator.hh>

#include <compare>
#include <concepts>
#include <optional>
#include <utility>

namespace lsm::utils {

// An immutable AVL tree
template<std::totally_ordered Key, typename Value>
class immutable_btree {
public:
    using generator
      = ss::coroutine::experimental::generator<std::pair<Key, Value>>;

    immutable_btree() = default;

    immutable_btree insert(Key key, Value value) {
        if (!_root) {
            return immutable_btree{
              ss::make_lw_shared<node>(std::move(key), std::move(value))};
        }
        return immutable_btree{_root->insert(std::move(key), std::move(value))};
    }

    template<typename KeyView>
    immutable_btree remove(KeyView key) {
        if (!_root) {
            return {};
        }
        return immutable_btree{_root->remove(key)};
    }

    template<typename KeyView>
    std::optional<Value> get(KeyView key) const {
        if (!_root) {
            return std::nullopt;
        }
        return _root->get(key);
    }

    // An in-order visitor to the tree
    generator iterator() const {
        if (!_root) {
            co_return;
        }
        auto gen = _root->iterator();
        while (auto result = co_await gen()) {
            co_yield *result;
        }
    }

private:
    struct node : public ss::enable_lw_shared_from_this<node> {
        node(Key k, Value v)
          : key(std::move(k))
          , value(std::move(v)) {}

        node(
          Key k,
          Value v,
          ss::lw_shared_ptr<node> l,
          ss::lw_shared_ptr<node> r,
          int64_t bf)
          : key(std::move(k))
          , value(std::move(v))
          , left(std::move(l))
          , right(std::move(r))
          , balance_factor(bf) {}

        Key key;
        Value value;
        ss::lw_shared_ptr<node> left;
        ss::lw_shared_ptr<node> right;
        int64_t balance_factor = 0;

        ss::lw_shared_ptr<node> copy() {
            return ss::make_lw_shared<node>(
              key, value, left, right, balance_factor);
        }

        ss::lw_shared_ptr<node> insert(Key new_key, Value new_value) {
            // TODO: balance
            std::strong_ordering cmp = new_key <=> key;
            if (cmp == std::strong_ordering::equal) {
                return ss::make_lw_shared<node>(
                  std::move(new_key),
                  std::move(new_value),
                  left,
                  right,
                  balance_factor);
            } else if (cmp == std::strong_ordering::less) {
                ss::lw_shared_ptr<node> new_left;
                if (left) {
                    new_left = left->insert(
                      std::move(new_key), std::move(new_value));
                } else {
                    new_left = ss::make_lw_shared<node>(
                      std::move(new_key), std::move(new_value));
                }
                return ss::make_lw_shared<node>(
                  key, value, new_left, right, balance_factor - 1);
            } else /* if (cmp == std::strong_ordering::greater) */ {
                ss::lw_shared_ptr<node> new_right;
                if (right) {
                    new_right = right->insert(
                      std::move(new_key), std::move(new_value));
                } else {
                    new_right = ss::make_lw_shared<node>(
                      std::move(new_key), std::move(new_value));
                }
                return ss::make_lw_shared<node>(
                  key, value, left, new_right, balance_factor + 1);
            }
        }

        template<typename KeyView>
        ss::lw_shared_ptr<node> remove(KeyView key_view) {
            // TODO: balance
            std::strong_ordering cmp = key_view <=> key;
            if (cmp == std::strong_ordering::equal) {
                // Delete this!
                if (right && left) {
                    return left->insert(right.key, right.value);
                } else if (right) {
                    return right;
                } else if (left) {
                    return left;
                } else {
                    return nullptr;
                }
            } else if (left && cmp == std::strong_ordering::less) {
                auto new_left = left->remove(key_view);
                if (new_left == left) {
                    return this->shared_from_this();
                }
                return ss::make_lw_shared<node>(
                  key, value, new_left, right, balance_factor + 1);
            } else if (right && cmp == std::strong_ordering::greater) {
                auto new_right = right->remove(key_view);
                if (new_right == right) {
                    return this->shared_from_this();
                }
                return ss::make_lw_shared<node>(
                  key, value, left, new_right, balance_factor - 1);
            }
            return this->shared_from_this();
        }

        ss::lw_shared_ptr<node> single_right_rotate() {
            auto a = copy();
            dassert(right, "expected node");
            auto b = right->copy();
            a->right = b->left;
            b->left = a;
            // TODO: update balance_factor
            return b;
        }
        ss::lw_shared_ptr<node> single_left_rotate() {
            auto a = copy();
            dassert(left, "expected node");
            auto b = left->copy();
            a->left = b->right;
            b->right = a;
            // TODO: update balance_factor
            return b;
        }
        ss::lw_shared_ptr<node> double_right_rotate() {
            auto a = copy();
            dassert(right && right->left, "expected node");
            auto b = a->right->left->copy();
            auto c = a->right->copy();
            a->right = b->left;
            c->left = b->right;
            b->left = a;
            b->right = c;
            // TODO: update balance_factor
        }

        ss::lw_shared_ptr<node> double_left_rotate() {
            auto a = copy();
            dassert(left && left->right, "expected node");
            auto b = a->left->right->copy();
            auto c = a->left->copy();
            a->left = b->right;
            c->right = b->left;
            b->right = a;
            b->left = c;
            // TODO: update balance_factor
        }

        template<typename KeyView>
        std::optional<Value> get(KeyView target) const {
            std::strong_ordering cmp = target <=> key;
            if (cmp == std::strong_ordering::equal) {
                return std::make_optional(value);
            } else if (left && cmp == std::strong_ordering::less) {
                return left->get(target);
            } else if (right && cmp == std::strong_ordering::greater) {
                return right->get(target);
            }
            return std::nullopt;
        }

        generator iterator() const {
            if (left) {
                auto gen = left->iterator();
                while (auto result = co_await gen()) {
                    co_yield *result;
                }
            }
            co_yield std::make_pair(key, value);
            if (right) {
                auto gen = right->iterator();
                while (auto result = co_await gen()) {
                    co_yield *result;
                }
            }
        }
    };

    explicit immutable_btree(ss::lw_shared_ptr<node> root)
      : _root(std::move(root)) {}

    ss::lw_shared_ptr<node> _root;
};

} // namespace lsm::utils
