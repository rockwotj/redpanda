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

#include "two_level_iterator.h"

#include <seastar/core/coroutine.hh>

namespace lsm::sst {

namespace {

class impl : public core::iterator {
public:
    impl(std::unique_ptr<core::iterator> index_iter, block_function block_fn)
      : _index_iter(std::move(index_iter))
      , _block_fn(std::move(block_fn)) {}

    ~impl() override = default;

    bool valid() const override { return _data_iter && _data_iter->valid(); }

    ss::future<> seek_to_first() override {
        co_await _index_iter->seek_to_first();
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek_to_first();
        }
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> seek_to_last() override {
        co_await _index_iter->seek_to_last();
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek_to_last();
        }
        co_await skip_empty_data_blocks_backward();
    }

    ss::future<> seek(core::internal_key_view target) override {
        co_await _index_iter->seek(target);
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek(target);
        }
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> next() override {
        assert(valid());
        co_await _data_iter->next();
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> prev() override {
        assert(valid());
        co_await _data_iter->prev();
        co_await skip_empty_data_blocks_backward();
    }

    core::internal_key_view key() override {
        assert(valid());
        return _data_iter->key();
    }

    iobuf value() override {
        assert(valid());
        return _data_iter->value();
    }

private:
    ss::future<> init_data_block() {
        if (!_index_iter->valid()) {
            _data_iter = nullptr;
            co_return;
        }
        auto handle = _index_iter->value();
        _data_iter = co_await _block_fn(std::move(handle));
    }
    ss::future<> skip_empty_data_blocks_forward() {
        while (!_data_iter || !_data_iter->valid()) {
            if (!_index_iter->valid()) {
                _data_iter = nullptr;
                co_return;
            }
            co_await _index_iter->next();
            co_await init_data_block();
            if (_data_iter) {
                co_await _data_iter->seek_to_first();
            }
        }
    }
    ss::future<> skip_empty_data_blocks_backward() {
        while (!_data_iter || !_data_iter->valid()) {
            if (!_index_iter->valid()) {
                _data_iter = nullptr;
                co_return;
            }
            co_await _index_iter->prev();
            co_await init_data_block();
            if (_data_iter) {
                co_await _data_iter->seek_to_last();
            }
        }
    }

    std::unique_ptr<core::iterator> _index_iter;
    block_function _block_fn;
    // May be nullptr
    std::unique_ptr<core::iterator> _data_iter;
};

} // namespace

std::unique_ptr<core::iterator> create_two_level_iterator(
  std::unique_ptr<core::iterator> index_iter, block_function block_fn) {
    return std::make_unique<impl>(std::move(index_iter), std::move(block_fn));
}

} // namespace lsm::sst
