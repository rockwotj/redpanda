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

#include <seastar/core/coroutine.hh>

#include <stdexcept>

namespace lsm {

namespace {

ss::lw_shared_ptr<internal::options> translate_options(options) {
    // TODO: implement me
    return ss::make_lw_shared<internal::options>();
}

model::offset seqno_cast(internal::sequence_number seqno) {
    return model::offset(static_cast<int64_t>(seqno()));
}

internal::sequence_number seqno_cast(model::offset o) {
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
      .key = target,
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
    return seqno_cast(_impl->max_persisted_seqno());
}

model::offset database::max_applied_offset() const {
    return seqno_cast(_impl->max_applied_seqno());
}

ss::future<> database::apply(write_batch batch) {
    auto b = std::move(batch._batch);
    co_await _impl->apply(std::move(*b));
}

ss::future<std::optional<iobuf>> database::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = target,
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

write_batch::write_batch()
  : _batch(std::make_unique<internal::write_batch>()) {}

write_batch::~write_batch() noexcept = default;

void write_batch::put(std::string_view key, iobuf value, model::offset offset) {
    auto k = internal::key::encode({
      .key = key,
      .seqno = seqno_cast(offset),
      .type = internal::value_type::value,
    });
    _batch->put(std::move(k), std::move(value));
}

void write_batch::remove(std::string_view key, model::offset offset) {
    auto k = internal::key::encode({
      .key = key,
      .seqno = seqno_cast(offset),
      .type = internal::value_type::tombstone,
    });
    _batch->remove(std::move(k));
}

} // namespace lsm
