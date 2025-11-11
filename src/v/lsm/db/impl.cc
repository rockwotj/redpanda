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

#include "lsm/db/impl.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/logger.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/db/iter.h"
#include "lsm/db/table_builder.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <exception>
#include <memory>
#include <utility>

namespace lsm::db {

using internal::operator""_level;

impl::impl(
  ctor,
  std::unique_ptr<io::persistence> p,
  ss::lw_shared_ptr<internal::options> o)
  : _persistence(std::move(p))
  , _opts(std::move(o))
  , _mem(ss::make_lw_shared<memtable>())
  , _table_cache(
      std::make_unique<table_cache>(
        _persistence.get(),
        _opts->max_open_files,
        ss::make_lw_shared<sst::block_cache>(
          _opts->block_cache_size / _opts->sst_block_size)))
  , _versions(
      std::make_unique<version_set>(
        _persistence.get(), _table_cache.get(), _opts)) {}

ss::future<std::unique_ptr<impl>> impl::open(
  ss::lw_shared_ptr<internal::options> opts,
  std::unique_ptr<io::persistence> persistence) {
    auto db = std::make_unique<impl>(
      ctor{}, std::move(persistence), std::move(opts));
    co_await db->recover();
    db->_background_work
      = ss::do_until(
          [db = db.get()] { return db->_as.abort_requested(); },
          [db = db.get()] {
              vlog(log.trace, "waiting for background work");
              return db->_start_background_work_signal.wait(db->_as)
                .then([db] {
                    db->_background_work_running = true;
                    vlog(log.trace, "start background compaction");
                    return db->run_background_compaction();
                })
                .then_wrapped([db](ss::future<> fut) {
                    db->_background_work_running = false;
                    db->maybe_schedule_compaction();
                    db->_background_work_finished_signal.broadcast();
                    try {
                        fut.get();
                        return;
                    } catch (const abort_requested_exception& ex) {
                        vlog(
                          log.debug,
                          "LSM background loop got abort request: {}",
                          ex.what());
                    } catch (const io_error_exception& ex) {
                        vlog(
                          log.warn,
                          "LSM background loop hit IO error: {}",
                          ex.what());
                    } catch (...) {
                        auto ep = std::current_exception();
                        vlog(
                          log.error,
                          "Unexpected error in LSM background loop: {}",
                          ep);
                    }
                    // Signal so that we immediately retry
                    // TODO(lsm): consider some kind of backoff or backpressure?
                    db->_start_background_work_signal.signal();
                });
          })
          .or_terminate();
    co_return db;
}

ss::future<> impl::apply(internal::write_batch batch) {
    if (batch.empty()) {
        co_return;
    }
    co_await make_room_for_write();
    _mem->apply(std::move(batch));
}

ss::future<> impl::make_room_for_write() {
    bool allow_delay = true;
    while (true) {
        if (
          allow_delay
          && _versions->current()->num_files(0_level)
               > _opts->level_zero_slowdown_writes_trigger) {
            // We're in throttling mode
            vlog(log.debug, "throttling writes due to number of L0 files");
            try {
                co_await ss::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                throw abort_requested_exception(
                  "shutdown requested during write throttling");
            }
            // Only throttle once.
            allow_delay = false;
            continue;
        }
        if (_mem->approximate_memory_usage() <= _opts->write_buffer_size) {
            // We're under our write buffer limit, let's proceed
            // Note there is a scheduling point here, so this is a soft
            // limit.
            co_return;
        }
        if (_imm) {
            vlog(log.warn, "blocking writes as in memory buffers are full");
            // We are over the write buffer limit and we have a pending
            // memtable flush, wait for it to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        if (
          _versions->current()->num_files(0_level)
          > _opts->level_zero_stop_writes_trigger) {
            vlog(
              log.warn,
              "too many L0 files, writing for compaction to finish before "
              "allowing more writes");
            // We've hit out L0 file limit, wait for compaction to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        // We're over our limit, let's make a new memtable
        vlog(log.trace, "scheduling memtable flush");
        _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
        maybe_schedule_compaction();
    }
}

ss::future<lookup_result> impl::get(internal::key_view key) {
    // Lookup in the mutable memtable
    {
        auto result = _mem->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the frozen memtable
    if (_imm) {
        auto result = (*_imm)->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the files
    auto current = _versions->current();
    version::get_stats stats{};
    auto result = co_await current->get(key, &stats);
    if (current->update_stats(stats)) {
        maybe_schedule_compaction();
    }
    co_return result;
}

ss::future<std::unique_ptr<internal::iterator>> impl::create_iterator() {
    auto iter = co_await create_internal_iterator();
    co_return create_db_iterator(
      std::move(iter),
      max_applied_seqno(),
      _opts,
      [this](internal::key_view key) {
          return _versions->current()->record_read_sample(key).then(
            [this](bool compaction_needed) {
                if (compaction_needed) {
                    maybe_schedule_compaction();
                }
            });
      });
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_internal_iterator() {
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.push_back(_mem->create_iterator());
    if (_imm) {
        list.push_back((*_imm)->create_iterator());
    }
    co_await _versions->current()->add_iterators(&list);
    co_return internal::create_merging_iterator(std::move(list));
}

ss::future<> impl::flush() {
    while (_imm) {
        co_await _background_work_finished_signal.wait(_as);
    }
    if (!_mem->empty()) {
        _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
        if (_background_work_running) {
            co_await _background_work_finished_signal.wait(_as);
        }
        maybe_schedule_compaction();
    }
    while (_imm) {
        co_await _background_work_finished_signal.wait(_as);
    }
}

ss::future<> impl::close() {
    _as.request_abort_ex(abort_requested_exception("database closing"));
    if (_background_work) {
        co_await *std::exchange(_background_work, std::nullopt);
    }
    co_await _table_cache->close();
    co_await _persistence->close();
}

ss::future<> impl::recover() {
    co_await _versions->recover();
    co_return;
}

void impl::maybe_schedule_compaction() {
    if (_background_work && _background_work_running) {
        return;
    }
    if (!_imm && !_versions->needs_compaction()) {
        return;
    }
    if (_as.abort_requested()) {
        return;
    }
    _start_background_work_signal.signal();
}

namespace {
struct compaction_state {
    struct output {
        internal::file_id id;
        uint64_t file_size = 0;
        internal::key smallest, largest;
        internal::sequence_number oldest, newest;
    };

    output& current_output() { return outputs.back(); }

    ss::future<> open_current_builder(
      internal::file_id id,
      io::persistence* p,
      ss::lw_shared_ptr<internal::options> opts) {
        outputs.emplace_back(id);
        auto w = co_await p->open_sequential_writer(
          internal::sst_file_name(id));
        builder.emplace(std::move(w), std::move(opts));
    }
    ss::future<> finish_current_builder() {
        auto b = std::exchange(builder, std::nullopt);
        co_await b->finish().finally([&b] { return b->close(); });
        uint64_t current_bytes = b->file_size();
        current_output().file_size = current_bytes;
        total_bytes += current_bytes;
    }

    std::exception_ptr err;
    chunked_vector<output> outputs;
    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <=
    // smallest_snapshot, we can drop all entries for the same key with
    // sequence numbers < S.
    internal::sequence_number smallest_snapshot;
    std::optional<sst::builder> builder;
    uint64_t total_bytes = 0;
};
} // namespace

ss::future<> impl::run_background_compaction() {
    if (_as.abort_requested()) {
        co_return;
    }
    if (_imm) {
        co_return co_await flush_memtable();
    }
    auto maybe_compaction = _versions->pick_compaction();
    if (!maybe_compaction) {
        co_return;
    }
    auto compaction = *std::move(maybe_compaction);
    if (compaction.is_trivial_move()) {
        auto input_level_files = compaction.num_input_files(
          compaction::which::input_level);
        vassert(
          input_level_files == 1,
          "trivial compactions should only be for a single input file: {}",
          input_level_files);
        auto file = compaction.input(compaction::which::input_level, 0);
        compaction.edit()->remove_file(compaction.level(), file->id);
        compaction.edit()->add_file({
          .level = compaction.level() + 1_level,
          .file_id = file->id,
          .file_size = file->file_size,
          .smallest = file->smallest,
          .largest = file->largest,
          .oldest_seqno = file->oldest_seqno,
          .newest_seqno = file->newest_seqno,
        });
        co_await _versions->log_and_apply(std::move(*compaction.edit()));
        co_return;
    }
    compaction_state state{
      // TODO: If we support snapshot reads we need to track the last seqno.
      .smallest_snapshot = _versions->last_seqno(),
    };
    try {
        auto input = co_await _versions->make_input_iterator(&compaction);
        std::optional<internal::key> current_key;
        internal::sequence_number last_seqno_for_key
          = internal::sequence_number::max();
        co_await input->seek_to_first();
        while (input->valid() && !_as.abort_requested()) {
            if (_imm) {
                // Always prioritize memtable flushes
                co_await flush_memtable();
                _background_work_finished_signal.signal();
            }
            auto key = input->key();
            if (state.builder && compaction.should_stop_before(key)) {
                co_await state.finish_current_builder();
            }
            bool drop = false;
            if (!current_key || key.user_key() != current_key->user_key()) {
                // First occurrence of this user key
                current_key = internal::key(key);
                last_seqno_for_key = internal::sequence_number::max();
            }
            auto key_seqno = key.seqno();
            // NOLINTNEXTLINE(*branch-clone*)
            if (last_seqno_for_key <= state.smallest_snapshot) {
                // Hidden by a newer entry for the same user key
                drop = true;
            } else if (
              key.is_tombstone() && key_seqno <= state.smallest_snapshot
              && compaction.is_base_level_for_key(key)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence
                // numbers (3) data in layers that are being compacted here
                // and have smaller sequence numbers will be dropped in the
                // next few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be
                // dropped.
                drop = true;
            }
            last_seqno_for_key = key_seqno;
            if (!drop) {
                if (!state.builder) {
                    co_await state.open_current_builder(
                      _versions->new_file_id(), _persistence.get(), _opts);
                }
                auto& current = state.current_output();
                if (state.builder->num_entries() == 0) {
                    current.smallest = internal::key(key);
                    current.oldest = key_seqno;
                    current.newest = key_seqno;
                }
                current.largest = internal::key(key);
                current.oldest = std::min(key_seqno, current.oldest);
                current.newest = std::max(key_seqno, current.newest);
                co_await state.builder->add(internal::key(key), input->value());
                // Close output file if it is big enough
                if (state.builder->file_size() >= _opts->target_file_size()) {
                    co_await state.finish_current_builder();
                }
            }
            co_await input->next();
        }
    } catch (const base_exception& ex) {
        state.err = std::make_exception_ptr(ex);
    }
    if (state.builder) {
        co_await state.finish_current_builder();
    }
    _as.check(); // Do this after we clean up the builder
    if (state.err) {
        std::rethrow_exception(state.err);
    }
    auto* edit = compaction.edit();
    compaction.add_input_deletions(edit);
    for (auto& output : state.outputs) {
        edit->add_file({
          .level = compaction.level() + 1_level,
          .file_id = output.id,
          .file_size = output.file_size,
          .smallest = std::move(output.smallest),
          .largest = std::move(output.largest),
          .oldest_seqno = output.oldest,
          .newest_seqno = output.newest,
        });
    }
    co_await _versions->log_and_apply(std::move(*edit));
    co_await remove_obsolete_files();
}

ss::future<> impl::flush_memtable() {
    vassert(_imm, "immutable memtable required in order to flush a memtable");
    auto v = _versions->current();
    auto id = _versions->new_file_id();
    auto result = co_await build_table(
      _persistence.get(), id, (*_imm)->create_iterator(), _opts, &_as);
    if (!result) {
        _versions->reuse_file_id(id);
        co_return;
    }
    auto level = v->pick_level_for_memtable_output(
      result->smallest, result->largest);
    version_edit edit(*_opts);
    edit.set_last_seqno(result->newest_seqno);
    edit.add_file({
      .level = level,
      .file_id = id,
      .file_size = result->file_size,
      .smallest = std::move(result->smallest),
      .largest = std::move(result->largest),
      .oldest_seqno = result->oldest_seqno,
      .newest_seqno = result->newest_seqno,
    });
    co_await _versions->log_and_apply(std::move(edit));
    // Now that the new version has been applied, it's safe to remove the
    // immutable memtable, as readers will pick up the new file instead.
    //
    // Note that due to the above scheduling point, it's possible for a
    // reader to pick up both the memtable and the new version with the
    // file. This is OK because all iterators deduplicate already.
    _imm = std::nullopt;
}

ss::future<> impl::remove_obsolete_files() {
    chunked_hash_set<internal::file_id> files = _versions->get_live_files();
    auto gen = _persistence->list_files();
    while (auto filename = co_await gen()) {
        auto parsed = internal::parse_filename(*filename);
        if (!parsed) {
            continue;
        }
        bool keep = true;
        switch (parsed->type) {
        case internal::file_type::manifest:
            keep = parsed->id >= _versions->current_manifest_id();
            break;
        case internal::file_type::sst:
            keep = files.contains(parsed->id);
            break;
        case internal::file_type::tmp:
            keep = false;
            break;
        case internal::file_type::current:
            keep = true;
            break;
        }
        if (!keep) {
            if (parsed->type == internal::file_type::sst) {
                co_await _table_cache->evict(parsed->id);
            }
            co_await _persistence->remove_file(*filename);
        }
    }
}

internal::sequence_number impl::max_persisted_seqno() const {
    return _versions->last_seqno();
}

internal::sequence_number impl::max_applied_seqno() const {
    return _mem->last_seqno()
      .or_else([this] { return _imm ? (*_imm)->last_seqno() : std::nullopt; })
      .value_or(max_persisted_seqno());
}

uint64_t impl::database_size() const {
    return _versions->current()->file_size_sum();
}

} // namespace lsm::db
