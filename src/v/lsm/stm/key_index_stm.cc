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

#include "lsm/io/disk_persistence.h"
#include "lsm/lsm.h"
#include "model/record.h"

namespace lsm {

ss::future<> key_index_stm::start() {
    auto p = co_await io::open_disk_persistence(_path);
    _db.emplace(co_await database::open({}, std::move(p)));
    set_next(model::next_offset(_db->max_applied_offset()));
}

ss::future<> key_index_stm::stop() {
    if (_db) {
        co_await _db->close().finally([this] { _db.reset(); });
    }
}

size_t key_index_stm::get_local_state_size() const {
    // TODO: Expose this from the LSM tree.
    return 0;
}

ss::future<> key_index_stm::remove_local_state() {
    // TODO: implement me
    co_return;
}

ss::future<std::optional<iobuf>>
key_index_stm::lookup_value(std::string_view key) {
    co_return co_await _db->get(key);
}

ss::future<> key_index_stm::apply(
  const model::record_batch& batch, const ssx::semaphore_units&) {
    if (batch.header().type != model::record_batch_type::raft_data) {
        co_return;
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
