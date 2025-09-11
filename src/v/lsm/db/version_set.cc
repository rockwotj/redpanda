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

#include "lsm/db/version_set.h"

#include "absl/container/btree_set.h"
#include "base/units.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/two_level_iterator.h"
#include "lsm/db/file_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace lsm::db {

namespace {

using internal::operator""_level;

// An internal iterator. For a given version/level pair, yields information
// about the files in the level. For a given entry, key() is the largest key
// that occurs in teh file, and value()  is an 16-byte value containing the file
// number and file size, both encoded using 64bit fixed encoding.
class level_file_num_iterator : public internal::iterator {
public:
    explicit level_file_num_iterator(
      chunked_vector<ss::lw_shared_ptr<file_meta_data>>* files)
      : _files(files)
      , _index(files->size()) {}

    bool valid() const override { return _index < _files->size(); }
    ss::future<> seek_to_first() override {
        _index = 0;
        co_return;
    }
    ss::future<> seek_to_last() override {
        _index = _files->empty() ? 0 : _files->size() - 1;
        co_return;
    }
    ss::future<> seek(internal::key_view target) override {
        _index = find_file(*_files, target);
        co_return;
    }
    ss::future<> next() override {
        ++_index;
        co_return;
    }
    ss::future<> prev() override {
        if (_index == 0) {
            _index = _files->size();
        } else {
            --_index;
        }
        co_return;
    }
    internal::key_view key() override { return (*_files)[_index]->largest; }
    iobuf value() override {
        iobuf v;
        auto placeholder = v.reserve(sizeof(uint64_t) * 2);
        const auto& f = (*_files)[_index];
        auto id = std::bit_cast<std::array<char, sizeof(uint64_t)>>(f->id);
        placeholder.write(id.data(), id.size());
        auto size = std::bit_cast<std::array<char, sizeof(uint64_t)>>(
          f->file_size);
        placeholder.write(size.data(), size.size());
        return v;
    }

private:
    chunked_vector<ss::lw_shared_ptr<file_meta_data>>* _files;
    uint32_t _index;
    iobuf _value_buf;
};

} // namespace

// A helper class to apply a sequence of edits to a version.
class version_set::builder {
public:
    builder(
      version_set* vset,
      ss::lw_shared_ptr<version> base,
      internal::options* opts)
      : _vset(vset)
      , _base(std::move(base))
      , _opts(opts)
      , _levels(_opts->levels.size()) {}

    void apply(const version_edit& edit) {
        for (internal::level level = 0_level;
             level() < edit._mutations_by_level.size();
             ++level) {
            const auto& mutation = edit._mutations_by_level[level];
            // Update compaction pointer
            if (mutation.compact_pointer) {
                _vset->_compact_pointer[level] = *mutation.compact_pointer;
            } else {
                _vset->_compact_pointer[level] = std::nullopt;
            }
            for (internal::file_id removed_file : mutation.removed_files) {
                _levels[level].removed_files.insert(removed_file);
            }
            for (const auto& added_file : mutation.added_files) {
                auto copy = ss::make_lw_shared(*added_file);
                // We arrange to automatically compact this file after a certain
                // number of seeks. Let's assume:
                // (1) One seek costs 10ms
                // (2) Writing or reading 1MiB costs 10ms (100MiB/s)
                // (3) A compaction of 1MiB does 25MiB of IO:
                //       1MiB read from this level
                //       10-12MiB read from next level (boundaries my be
                //       misaligned)
                //       10-12MiB written to next level
                // This imples that 25 seeks cost the same as the compaction of
                // 1MB of data. I.e., one seek costs approximately the same as
                // the compaction of 40KiB of data. We are a little conservative
                // and allow approximately ne seek for every 16KiB of data
                // before triggering a compaction.
                copy->allowed_seeks = static_cast<int32_t>(
                  copy->file_size / 16_KiB);
                constexpr static int32_t min_allowed_seeks = 100;
                if (copy->allowed_seeks < min_allowed_seeks) {
                    copy->allowed_seeks = min_allowed_seeks;
                }
                _levels[level].removed_files.erase(copy->id);
                _levels[level].added_files.insert(copy);
            }
        }
    }

    void save_to(version* v) {
        by_smallest_key cmp;
        for (const auto& level : _opts->levels) {
            // Merge the set of added files with the set of pre-existing file.
            // Drop any deleted files. Store the result in *v.
            const auto& base_files = _base->_files[level.number];
            auto base_iter = base_files.begin();
            auto base_end = base_files.end();
            auto& state = _levels[level.number];
            for (const auto& added_file : state.added_files) {
                // Add all smaller files listed in base_
                auto bpos = std::upper_bound(
                  base_iter, base_end, added_file, cmp);
                for (; base_iter != bpos; ++base_iter) {
                    maybe_add_file(v, level.number, *base_iter);
                }
                maybe_add_file(v, level.number, added_file);
            }
            // Add all remaining base files
            for (; base_iter != base_end; ++base_iter) {
                maybe_add_file(v, level.number, *base_iter);
            }
#ifndef NDEBUG
            if (level.number > 0_level) {
                const auto& files = v->_files[level.number];
                for (uint32_t i = 1; i < files.size(); ++i) {
                    const auto& prev_end = files[i - 1]->largest;
                    const auto& this_begin = files[i]->smallest;
                    dassert(
                      prev_end < this_begin,
                      "overlapping ranges in level {}: {} <= {}",
                      level.number,
                      prev_end,
                      this_begin);
                }
            }
#endif
        }
    }

private:
    void maybe_add_file(
      version* v,
      internal::level level,
      ss::lw_shared_ptr<file_meta_data> file) {
        if (_levels[level].removed_files.contains(file->id)) {
            return;
        }
        auto& files = v->_files[level];
        if (level > 0_level && !files.empty()) {
            dassert(
              files.back()->largest < file->smallest,
              "expected no overlap between files, got: {} >= {}",
              files.back()->largest,
              file->smallest);
        }
        files.push_back(std::move(file));
    }

    version_set* _vset;
    ss::lw_shared_ptr<version> _base;
    internal::options* _opts;
    struct level_state {
        chunked_hash_set<internal::file_id> removed_files;
        absl::btree_set<ss::lw_shared_ptr<file_meta_data>, by_smallest_key>
          added_files;
    };
    absl::FixedArray<level_state> _levels;
};

version::version(ctor, version_set* vset)
  : _vset(vset)
  , _files(_vset->_options.levels.size()) {}

void version::add_iterators(chunked_vector<internal::iterator>* iters) {
    // Merge all level zero files together since they may overlap.
    for (const auto& file : _files[0_level]) {
        iters->push_back(
          _vset->_table_cache->create_iterator(file->id, file->file_size));
    }
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    for (const auto& level : std::span(_vset->_options.levels).subspan(1)) {
        if (_files[level.number].empty()) {
            continue;
        }
        iters->push_back(create_concatenating_iterator(level.number));
    }
}

bool version::update_stats(const get_stats& stats) {
    if (stats.seek_file) {
        const auto& f = *stats.seek_file;
        --f->allowed_seeks;
        if (f->allowed_seeks <= 0 && !_file_to_compact) {
            _file_to_compact = f;
            _file_to_compact_level = stats.seek_file_level;
            return true;
        }
    }
    return false;
}

bool version::record_read_sample(internal::key_view key) {}

chunked_vector<ss::lw_shared_ptr<file_meta_data>>
version::get_overlapping_inputs(
  internal::level,
  const internal::key_view* begin,
  const internal::key_view* end) {}

ss::future<std::optional<iobuf>> version::get(internal::key_view) {}

bool version::overlap_in_level(
  internal::level,
  const internal::key_view* begin,
  const internal::key_view* end) {}

internal::level version::pick_level_for_memtable_output(
  internal::key_view begin, internal::key_view end) {}

std::unique_ptr<internal::iterator>
version::create_concatenating_iterator(internal::level level) {
    // TODO(lsm): verify that these are ok to be non-owning pointers.
    auto index_iter = std::make_unique<level_file_num_iterator>(&_files[level]);
    return internal::create_two_level_iterator(
      std::move(index_iter),
      [this](iobuf value) -> ss::future<std::unique_ptr<internal::iterator>> {
          const auto& fragment = *value.begin();
          auto it = fragment.get();
          internal::file_id id;
          std::memcpy(&id, it, sizeof(id));
          uint64_t file_size = 0;
          std::advance(it, sizeof(id));
          std::memcpy(&file_size, it, sizeof(file_size));
          return _vset->_table_cache->create_iterator(id, file_size);
      });
}

fmt::iterator version::format_to(fmt::iterator it) const {
    // For example:
    //   --- level 1 ---
    //   17:234['a' .. 'e']
    //   20:31['a' .. 'e']
    for (size_t level = 0; level < _files.size(); ++level) {
        it = fmt::format_to(it, "--- level {} ---\n", level);
        for (const auto& file : _files[level]) {
            it = fmt::format_to(
              it,
              "{}:{}['{}' .. '{}']\n",
              file->id,
              file->file_size,
              file->smallest,
              file->largest);
        }
    }
    return it;
}

version_set::version_set(
  io::persistence* persistence,
  table_cache* table_cache,
  internal::options opts)
  : _persistence(persistence)
  , _table_cache(table_cache)
  , _options(std::move(opts))
  , _compact_pointer(_options.levels.size()) {
    set_current(ss::make_lw_shared<version>(version::ctor{}, this));
}

void version_set::set_current(ss::lw_shared_ptr<version> new_version) {
    weak_intrusive_list<version>::push_front(&_current, std::move(new_version));
}

ss::future<> version_set::log_and_apply(version_edit edit) {
    edit.set_next_file_id(_next_file_id);
    edit.set_last_seq_num(_last_seqno);
    auto v = ss::make_lw_shared<version>(version::ctor{}, this);
    {
        version_set::builder builder(this, _current, &_options);
        builder.apply(edit);
        builder.save_to(v.get());
    }
    finalize(v.get());
    // This is where we diverge a bit from LevelDB. We don't log manifest
    // deltas, but just snapshot the full manifest. At somepoint we will want
    // delta writes, but for now we will just write full snapshots.
    auto manifest_filename = internal::manifest_file_name(_manifest_id);
    auto file = co_await _persistence->open_sequential_writer(
      manifest_filename);
    auto fut = co_await ss::coroutine::as_future<>(
      write_manifest(v.get(), file.get()));
    co_await file->close();
    if (fut.failed()) {
        co_await _persistence->remove_file(manifest_filename);
        std::rethrow_exception(fut.get_exception());
    }
    set_current(std::move(v));
}

void version_set::finalize(version* v) {
    // Precompute the best level for the next compaction
    internal::level best_level = 0_level;
    double best_score = static_cast<double>(v->_files[best_level].size())
                        / static_cast<double>(
                          _options.default_level_one_compaction_trigger);
    for (const auto& level : std::span(_options.levels).subspan(1)) {
        size_t level_bytes = total_file_size(v->_files[level.number]);
        double score = static_cast<double>(level_bytes)
                       / static_cast<double>(max_bytes_for_level(level.number));
        if (score > best_score) {
            best_level = level.number;
            best_score = score;
        }
    }
    v->_compaction_level = best_level;
    v->_compaction_score = best_score;
}

ss::future<>
version_set::write_manifest(version*, io::sequential_file_writer*) {
    // TODO: implement me!
    co_return;
}

} // namespace lsm::db
