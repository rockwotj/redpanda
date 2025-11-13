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

#include "lsm/stm/key_index_stm.h"

#include "lsm/core/internal/logger.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/lsm.h"
#include "model/record.h"

#include <seastar/util/file.hh>

#include <stdexcept>

namespace lsm {

namespace {

struct lookup_consumer {
public:
    explicit lookup_consumer(model::offset target)
      : _target(target) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        auto iter = model::record_batch_iterator::create(b);
        int64_t delta = _target - b.base_offset();
        while (iter.has_next()) {
            auto record = iter.next();
            if (record.offset_delta() == delta) {
                _result.emplace(record.release_value());
                break;
            }
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    std::optional<iobuf> end_of_stream() { return std::move(_result); }

private:
    model::offset _target;
    std::optional<iobuf> _result;
};

} // namespace

ss::future<> key_index_stm::start() {
    if (_db) [[unlikely]] {
        co_return;
    }
    vlog(log.info, "starting key index stm for {}", _raft->ntp());
    auto p = co_await io::open_disk_persistence(_path);
    _db.emplace(co_await database::open({}, std::move(p)));
    set_next(model::next_offset(_db->max_applied_offset()));
}

ss::future<> key_index_stm::stop() {
    vlog(log.info, "stopping key index stm for {}", _raft->ntp());
    if (auto db = std::exchange(_db, std::nullopt)) {
        co_await db->close();
    }
}

size_t key_index_stm::get_local_state_size() const {
    return _db.transform([](auto& db) { return db.database_size(); })
      .value_or(0);
}

ss::future<> key_index_stm::remove_local_state() {
    if (auto db = std::exchange(_db, std::nullopt)) {
        co_await db->close();
    }
    co_await ss::recursive_remove_directory(_path);
}

ss::future<std::optional<iobuf>>
key_index_stm::lookup_value(std::string_view key) {
    if (!_db) [[unlikely]] {
        throw ss::abort_requested_exception();
    }
    auto maybe_offset = co_await _db->get(key);
    if (!maybe_offset) {
        co_return std::nullopt;
    }
    auto offset = serde::from_iobuf<model::offset>(std::move(*maybe_offset));
    storage::local_log_reader_config reader_config(
      /*start_offset=*/offset,
      /*max_offset=*/offset,
      /*max_bytes=*/1,
      /*type_filter=*/std::nullopt,
      /*time=*/std::nullopt,
      /*as=*/std::nullopt);
    auto reader = co_await _raft->make_reader(reader_config);
    co_return co_await reader.consume(
      lookup_consumer(offset), model::no_timeout);
}

ss::future<> key_index_stm::apply(
  const model::record_batch& batch, const ssx::semaphore_units&) {
    if (batch.header().type != model::record_batch_type::raft_data) {
        co_return;
    }
    if (batch.header().attrs.is_control()) {
        co_return;
    }
    if (!_db) [[unlikely]] {
        throw std::runtime_error(
          "trying to apply a batch to an LSM database that is closed");
    }
    write_batch writes;
    auto iter = model::record_batch_iterator::create(batch);
    while (iter.has_next()) {
        auto record = iter.next();
        static constexpr int32_t max_key_size = 16_KiB;
        if (auto ksz = record.key_size(); ksz < 0 || ksz > max_key_size) {
            continue;
        }
        auto key = record.key().linearize_to_string();
        auto offset = batch.base_offset()
                      + model::offset_delta(record.offset_delta());
        if (record.is_tombstone()) {
            writes.remove(key, offset);
        } else {
            // TODO: instead of writing the offset as the value, we should
            // support being able to read the offset directly from it being
            // encoded in the internal::key.
            writes.put(key, serde::to_iobuf(offset), offset);
        }
    }
    co_await _db->apply(std::move(writes));
}

} // namespace lsm
