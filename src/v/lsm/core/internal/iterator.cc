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

#include "lsm/core/internal/iterator.h"

namespace lsm::internal {

namespace {
class empty_iterator final : public iterator {
    bool valid() const final { return false; }
    ss::future<> seek_to_first() final { return ss::now(); }
    ss::future<> seek_to_last() final { return ss::now(); }
    ss::future<> seek(key_view) final { return ss::now(); }
    ss::future<> next() final {
        throw std::runtime_error("next() called on empty iterator");
    }
    ss::future<> prev() final {
        throw std::runtime_error("prev() called on empty iterator");
    }
    key_view key() final {
        throw std::runtime_error("key() called on empty iterator");
    }
    iobuf value() final {
        throw std::runtime_error("value() called on empty iterator");
    }
};
} // namespace

std::unique_ptr<iterator> iterator::create_empty() {
    return std::make_unique<empty_iterator>();
}

} // namespace lsm::internal
