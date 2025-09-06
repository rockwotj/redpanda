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

#include "lsm/sst/table.h"

#include "lsm/block/block.h"
#include "lsm/block/contents.h"
#include "lsm/block/filter.h"
#include "lsm/block/handle.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/footer.h"
#include "two_level_iterator.h"

#include <seastar/core/coroutine.hh>

namespace lsm::sst {

namespace {

ss::future<std::optional<block::filter_reader>>
read_filter(io::random_access_file_reader* file, block::block metaindex_block) {
    auto iter = metaindex_block.create_iterator();
    auto key = core::internal_key::encode({.key = "filter.RedpandaBloomV0"});
    co_await iter->seek(key);
    if (!iter->valid() || iter->key() != key) {
        co_return std::nullopt;
    }
    auto filter_handle = block::handle::from_iobuf(iter->value());
    auto filter_contents = co_await block::contents::read(file, filter_handle);
    co_return block::filter_reader(std::move(filter_contents));
}

} // namespace

class table::impl {
public:
    impl(
      block::block index_block,
      std::unique_ptr<io::random_access_file_reader> file,
      std::optional<block::filter_reader> filter)
      : _file(std::move(file))
      , _index_block(std::move(index_block))
      , _filter(std::move(filter)) {}

    std::unique_ptr<core::iterator> create_iterator() {
        return create_two_level_iterator(
          _index_block.create_iterator(), [this](iobuf index_value) {
              return block_reader(std::move(index_value));
          });
    }
    ss::future<> internal_get(
      core::internal_key_view key,
      absl::FunctionRef<ss::future<>(core::internal_key_view, iobuf)> fn) {
        auto iiter = _index_block.create_iterator();
        co_await iiter->seek(key);
        if (!iiter->valid()) {
            co_return;
        }
        auto v = iiter->value();
        if (_filter) {
            auto handle = block::handle::from_iobuf(v.share());
            if (!_filter->key_may_match(handle.offset, key)) {
                // Bloom filter says it's certainly not there.
                co_return;
            }
        }
        auto block_iter = co_await block_reader(std::move(v));
        co_await block_iter->seek(key);
        if (block_iter->valid()) {
            co_await fn(block_iter->key(), block_iter->value());
        }
    }

private:
    ss::future<std::unique_ptr<core::iterator>>
    block_reader(iobuf index_value) {
        auto handle = block::handle::from_iobuf(std::move(index_value));
        // TODO(lsm): use block cache here
        auto contents = co_await block::contents::read(_file.get(), handle);
        co_return block::block(std::move(contents)).create_iterator();
    }

    std::unique_ptr<io::random_access_file_reader> _file;
    block::block _index_block;
    std::optional<block::filter_reader> _filter;
};

table::table(std::unique_ptr<impl> impl)
  : _impl(std::move(impl)) {}

table::~table() = default;

ss::future<table> table::open(
  std::unique_ptr<io::random_access_file_reader> file, size_t file_size) {
    if (file_size < footer::encoded_length) {
        throw std::runtime_error(
          fmt::format("file is too short to be an sstable"));
    }
    auto encoded_footer = co_await file->read(
      file_size - footer::encoded_length, footer::encoded_length);
    auto footer = footer::from_iobuf(encoded_footer.as_iobuf());
    auto index_block_contents = co_await block::contents::read(
      file.get(), footer.index_handle);
    auto metaindex_block_contents = co_await block::contents::read(
      file.get(), footer.metaindex_handle);

    block::block index_block(std::move(index_block_contents));

    block::block metaindex_block(std::move(metaindex_block_contents));
    auto filter = co_await read_filter(file.get(), metaindex_block);
    co_return table(
      std::make_unique<impl>(
        std::move(index_block), std::move(file), std::move(filter)));
}

std::unique_ptr<core::iterator> table::create_iterator() {
    return _impl->create_iterator();
}

ss::future<> table::internal_get(
  core::internal_key_view key,
  absl::FunctionRef<ss::future<>(core::internal_key_view, iobuf)> fn) {
    return _impl->internal_get(key, fn);
}
} // namespace lsm::sst
