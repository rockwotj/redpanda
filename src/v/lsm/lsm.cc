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

#include "lsm/lsm.h"

#include "lsm/core/internal/batch.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/db/impl.h"
#include "model/batch_compression.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>

#include <stdexcept>

namespace lsm {

namespace {

ss::lw_shared_ptr<internal::options> translate_options(options) {
    // TODO: implement me
    return ss::make_lw_shared<internal::options>();
}

model::offset translate_seqno(internal::sequence_number seqno) {
    return model::offset(static_cast<int64_t>(seqno()));
}

internal::sequence_number translate_offset(model::offset o) {
    if (o < model::offset{0}) {
        throw std::invalid_argument(
          fmt::format(
            "unable to translate negative offset {} into a sequence number",
            o));
    }
    return internal::sequence_number(static_cast<uint64_t>(o()));
}

} // namespace

iterator::iterator(std::unique_ptr<internal::iterator> impl)
  : _impl(std::move(impl)) {}

iterator::~iterator() noexcept = default;

bool iterator::valid() const { return _impl->valid(); }

ss::future<> iterator::seek_to_first() { return _impl->seek_to_first(); }
ss::future<> iterator::seek_to_last() { return _impl->seek_to_last(); }
ss::future<> iterator::seek(std::string_view target) {
    auto key = internal::key::encode({
      .key = ss::sstring(target),
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    co_await _impl->seek(key);
}
ss::future<> iterator::next() { return _impl->next(); }
ss::future<> iterator::prev() { return _impl->prev(); }
std::string_view iterator::key() { return _impl->key().user_key(); }
iobuf iterator::value() { return _impl->value(); }

database::database(std::unique_ptr<db::impl> impl)
  : _impl(std::move(impl)) {}

database::~database() noexcept = default;

ss::future<database>
database::open(options opts, std::unique_ptr<io::persistence> p) {
    auto impl = co_await db::impl::open(translate_options(opts), std::move(p));
    co_return database(std::move(impl));
}

ss::future<> database::close() { return _impl->close(); }

model::offset database::max_persisted_offset() const {
    return translate_seqno(_impl->max_persisted_seqno());
}

ss::future<> database::apply(model::record_batch records) {
    constexpr static int32_t max_key_size = 32_KiB;
    if (records.compressed()) {
        records = co_await model::decompress_batch(records);
    }
    auto iter = model::record_batch_iterator::create(records);
    internal::write_batch batch;
    while (iter.has_next()) {
        auto record = iter.next();
        if (!record.has_key() || record.key_size() > max_key_size) {
            continue;
        }
        auto seqno = translate_offset(
          records.base_offset() + model::offset_delta(record.offset_delta()));
        internal::key key = internal::key::encode({
          .key = record.key().linearize(),
          .seqno = seqno,
          .type = record.is_tombstone() ? internal::value_type::tombstone
                                        : internal::value_type::value,
        });
        if (record.is_tombstone()) {
            batch.remove(key);
        } else {
            batch.put(key, record.release_value());
        }
    }
    co_await _impl->apply(batch);
}

ss::future<std::optional<iobuf>> database::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = ss::sstring(target),
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    auto result = co_await _impl->get(key);
    co_return result.take_value();
}

ss::future<iterator> database::create_iterator() {
    auto iter = co_await _impl->create_iterator();
    co_return iterator(std::move(iter));
}

} // namespace lsm
