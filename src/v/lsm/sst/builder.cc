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

ss::future<> builder::add(ss::sstring key, iobuf value) {
    _block.add(std::move(key), std::move(value));
    if (_block.current_size_estimate() > _opts.block_size) {
        co_await flush();
    }
}

ss::future<> builder::flush() {
    if (_block.empty()) {
        co_return;
    }
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    auto buf = _block.finish();
}

ss::future<block::handle>
builder::write_raw_block(iobuf buf, compression::type comp_type) {
    if (comp_type != compression::type::none) {
        buf = co_await compression::stream_compressor::compress(
          std::move(buf), _opts.compression);
    }
    // Make sure the CRC covers the type
    buf.append(std::to_array({static_cast<uint8_t>(_opts.compression)}));
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
          _filter->finish(), compression::type::none);
    }

    // write metaindex block
    block::builder meta_index_block;
    if (_filter) {
        ss::sstring key = "filter.RedpandaBloomV0";
        meta_index_block.add(key, filter_block_handle.as_iobuf());
    }
    metaindex_block_handle = co_await write_raw_block(
      meta_index_block.finish(), compression::type::none);

    // write index block
    block::builder index_block;
    if (_filter) {
        // r->options.comparator->FindShortSuccessor(&r->last_key);
        // std::string handle_encoding;
        // r->pending_handle.EncodeTo(&handle_encoding);
        // r->index_block.Add(r->last_key, Slice(handle_encoding));
        // r->pending_index_entry = false;
    }
    index_block_handle = co_await write_raw_block(
      index_block.finish(), compression::type::none);

    // write footer

    co_return;
}

ss::future<> builder::close() { return _output.close(); }

} // namespace lsm::sst
