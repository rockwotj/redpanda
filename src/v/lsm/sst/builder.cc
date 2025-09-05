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

#include "lsm/sst/builder.h"

#include "bytes/iostream.h"
#include "hashing/crc32c.h"
#include "lsm/sst/footer.h"

#include <seastar/core/fstream.hh>

namespace lsm::sst {

ss::future<builder> builder::create(ss::file f, options opts) {
    auto stream = co_await ss::make_file_output_stream(
      f, ss::file_output_stream_options{});
    co_return builder{std::move(stream), opts};
}

builder::builder(ss::output_stream<char>&& os, options opts)
  : _output(std::move(os))
  , _opts(opts) {}

ss::future<> builder::add(core::internal_key key, iobuf value) {
    if (_pending_index_entry) {
        // TODO(lsm): We can compute shorter block boundaries for our index
        // here. For example: consider a block that ends with "the quick brown
        // fox" and the next block starts with "the who". In this case, we can
        // encode the index entry with "the r" because it's >= everything in the
        // previous block and < all entries in the next block.
        _index_block.add(_last_key, _pending_handle.as_iobuf());
        _pending_index_entry = false;
    }
    if (_filter) {
        _filter->add_key(key);
    }
    // TODO(lsm): It's a bummer we make so many copies of the key here
    // we only need it when we get to a block boundary (or at the end of the
    // stream). It might be better to only set the last key when we're about
    // to flush, otherwise we could decode the last key in the last _data_block
    // as well. For now, just copy leveldb and do the simple copy.
    _last_key = key;
    _data_block.add(std::move(key), std::move(value));
    ++_added_entries;
    if (_data_block.current_size_estimate() > _opts.block_size) {
        co_await flush();
    }
}

ss::future<> builder::flush() {
    if (_data_block.empty()) {
        co_return;
    }
    auto buf = _data_block.finish();
    _pending_handle = co_await write_raw_block(
      std::move(buf), _opts.compression);
    _pending_index_entry = true;
    if (_filter) {
        _filter->start_block(_written_bytes);
    }
}

ss::future<block::handle>
builder::write_raw_block(iobuf buf, compression_type comp_type) {
    if (comp_type != compression_type::none) {
        // TODO(lsm): only use compressed version if it actually saves enough
        // bytes.
        buf = co_await compress(std::move(buf), comp_type);
    }
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    // Make sure the CRC covers the type
    buf.append(std::to_array({std::to_underlying(comp_type)}));
    crc::crc32c crc;
    crc_extend_iobuf(crc, buf);
    buf.append(
      std::bit_cast<std::array<uint8_t, sizeof(crc.value())>>(
        crc::mask(crc.value())));
    block::handle h = {.offset = _written_bytes, .size = buf.size_bytes()};
    _written_bytes += h.size;
    co_await write_iobuf_to_output_stream(std::move(buf), _output);
    co_return h;
}

ss::future<> builder::finish() {
    co_await flush();

    block::handle filter_block_handle, metaindex_block_handle,
      index_block_handle;

    if (_filter) {
        filter_block_handle = co_await write_raw_block(
          _filter->finish(), compression_type::none);
    }

    // write metaindex block
    block::builder meta_index_block;
    if (_filter) {
        auto key = core::internal_key::encode(
          {.key = "filter.RedpandaBloomV0"});
        meta_index_block.add(std::move(key), filter_block_handle.as_iobuf());
    }
    metaindex_block_handle = co_await write_raw_block(
      meta_index_block.finish(), _opts.compression);

    if (_pending_index_entry) {
        // TODO(lsm): See the TODO in builder::add
        _index_block.add(_last_key, _pending_handle.as_iobuf());
        _pending_index_entry = false;
    }
    index_block_handle = co_await write_raw_block(
      _index_block.finish(), compression_type::none);

    // write footer
    iobuf encoded_footer = footer{
      .metaindex_handle = metaindex_block_handle,
      .index_handle = index_block_handle,
    }.as_iobuf();
    _written_bytes += encoded_footer.size_bytes();
    co_await write_iobuf_to_output_stream(std::move(encoded_footer), _output);
}

size_t builder::num_entries() const { return _added_entries; }

size_t builder::file_size() const { return _written_bytes; }

ss::future<> builder::close() { return _output.close(); }

} // namespace lsm::sst
