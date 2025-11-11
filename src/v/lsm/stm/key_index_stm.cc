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
    co_return co_await _db->get(key);
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
            // TODO: instead of writing in the LSM tree, instead write
            // nothing and get back the offset, then read the offset
            writes.put(key, record.release_value(), offset);
        }
    }
    co_await _db->apply(std::move(writes));
}

} // namespace lsm
