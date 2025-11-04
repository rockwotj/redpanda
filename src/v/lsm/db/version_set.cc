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
#include "container/chunked_vector.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/core/internal/two_level_iterator.h"
#include "lsm/db/file_utils.h"
#include "lsm/db/manifest.proto.h"
#include "lsm/db/version_edit.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace lsm::db {

namespace {

using internal::operator""_level;
using internal::operator""_file_id;

// An internal iterator. For a given version/level pair, yields information
// about the files in the level. For a given entry, key() is the largest key
// that occurs in teh file, and value()  is an 16-byte value containing the file
// number and file size, both encoded using 64bit fixed encoding.
//
// NOTE: It's up to the user of this class to ensure the files pointer is kept
// alive.
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

ss::future<std::optional<ss::sstring>> read_current_file(io::persistence* p) {
    auto maybe_file = co_await p->open_sequential_reader(
      internal::current_file_name());
    if (!maybe_file) {
        co_return std::nullopt;
    }
    auto file = std::move(*maybe_file);
    constexpr static size_t buffer_size = 4_KiB;
    auto fut = co_await ss::coroutine::as_future<iobuf>(
      file->read(buffer_size));
    co_await file->close();
    auto buf = fut.get();
    if (buf.size_bytes() >= buffer_size) {
        throw corruption_exception(
          "expected {} file to be less than {} bytes",
          internal::current_file_name(),
          buffer_size);
    }
    ss::sstring contents;
    for (const auto& frag : buf) {
        contents.append(frag.get(), frag.size());
    }
    co_return contents;
}

} // namespace

// A helper class to apply a sequence of edits to a version.
class version_set::builder {
public:
    builder(version_set* vset, ss::lw_shared_ptr<version> base)
      : _vset(vset)
      , _base(std::move(base))
      , _levels(_vset->_options->levels.size()) {}

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
                copy->allowed_seeks = static_cast<int32_t>(
                  copy->file_size / _vset->_options->compact_after_seek_bytes);
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
        for (const auto& level : _vset->_options->levels) {
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
    struct level_state {
        chunked_hash_set<internal::file_id> removed_files;
        absl::btree_set<ss::lw_shared_ptr<file_meta_data>, by_smallest_key>
          added_files;
    };
    absl::FixedArray<level_state> _levels;
};

version::version(ctor, version_set* vset)
  : _vset(vset)
  , _files(_vset->_options->levels.size()) {}

ss::future<> version::add_iterators(
  chunked_vector<std::unique_ptr<internal::iterator>>* iters) {
    // Merge all level zero files together since they may overlap.
    for (const auto& file : _files[0_level]) {
        auto iter = co_await _vset->_table_cache->create_iterator(
          file->id, file->file_size);
        iters->push_back(std::move(iter));
    }
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    for (const auto& level : std::span(_vset->_options->levels).subspan(1)) {
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

ss::future<bool> version::record_read_sample(internal::key_view key) {
    get_stats stats;
    size_t matches = 0;
    co_await for_each_overlapping(
      key,
      [&stats, &matches](
        internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
          ++matches;
          if (matches == 1) {
              stats.seek_file = std::move(file);
              stats.seek_file_level = level;
          }
          return ss::make_ready_future<ss::stop_iteration>(matches >= 2);
      });
    // Must have at least two matches since we want to merge across files. But
    // what if we have a single file that contains many overwrites and
    // deletions? Should we have another mechanism for finding such files?
    if (matches >= 2) {
        // 1MiB cost is about 1 seek (see comment in
        // options::compact_after_seek_bytes).
        co_return update_stats(stats);
    }
    co_return false;
}

chunked_vector<ss::lw_shared_ptr<file_meta_data>>
version::get_overlapping_inputs(
  internal::level level,
  std::optional<internal::key_view> begin,
  std::optional<internal::key_view> end) {
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> inputs;
    const auto& files = _files[level];
    for (size_t i = 0; i < files.size(); ++i) {
        const auto& file = files[i];
        if (begin && file->largest < *begin) { // NOLINT(*branch-clone*)
            // file is completely before specified range; skip it
        } else if (end && file->smallest > *end) {
            // file is completely after specified range; skip it
        } else {
            inputs.push_back(file);
            // Level 0 files may overlap each over. So check if the newly added
            // file has expanded the range. If so, restart search.
            if (level == 0_level) {
                if (begin && file->smallest < *begin) {
                    begin = file->smallest;
                    inputs.clear();
                    i = 0;
                } else if (end && file->largest > *end) {
                    end = file->largest;
                    inputs.clear();
                    i = 0;
                }
            }
        }
    }
    return inputs;
}

namespace {

struct found_value {
    internal::key key;
    iobuf value;
};

struct lookup_state {
    std::optional<found_value> found;
    version::get_stats last_file_read;
    version::get_stats* stats;
    table_cache* table_cache;
    internal::key_view target;

    ss::future<ss::stop_iteration>
    on_file(internal::level level, ss::lw_shared_ptr<file_meta_data> file);
};

ss::future<ss::stop_iteration> lookup_state::on_file(
  internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
    if (!stats->seek_file && last_file_read.seek_file) {
        *stats = last_file_read;
    }
    last_file_read.seek_file = file;
    last_file_read.seek_file_level = level;
    co_await table_cache->get(
      file->id,
      file->file_size,
      target,
      [this](internal::key_view key, iobuf value) {
          if (key.user_key() != target.user_key()) {
              return ss::now();
          }
          found.emplace(internal::key{key}, std::move(value));
          return ss::now();
      });
    co_return found ? ss::stop_iteration::yes : ss::stop_iteration::no;
}

} // namespace

ss::future<lookup_result>
version::get(internal::key_view target, get_stats* stats) {
    stats->seek_file = std::nullopt;
    lookup_state state{
      .stats = stats,
      .table_cache = _vset->_table_cache,
      .target = target,
    };
    co_await for_each_overlapping(
      target,
      [&state](internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
          return state.on_file(level, file);
      });
    if (!state.found) {
        co_return lookup_result::missing();
    } else if (state.found->key.is_tombstone()) {
        co_return lookup_result::tombstone();
    }
    co_return lookup_result::value(std::move(state.found->value));
}

bool version::overlap_in_level(
  internal::level level,
  std::optional<internal::key_view> begin,
  std::optional<internal::key_view> end) {
    return some_file_overlaps_range(level > 0_level, _files[level], begin, end);
}

internal::level version::pick_level_for_memtable_output(
  internal::key_view begin, internal::key_view end) {
    auto level = 0_level;
    if (!overlap_in_level(level, begin, end)) {
        // Push to next level if there is no overlap in next level,
        // and the bytes overlapping in the level after that are limited.
        // As we try to skip expensive level 0=>1 compactions if possible
        constexpr static auto max_mem_compact_level = 1_level;
        while (level <= max_mem_compact_level) {
            if (overlap_in_level(level + 1_level, begin, end)) {
                break;
            }
            if (level() + 2 < _vset->_options->levels.size()) {
                // Check that file does not overlap too many grandparent
                // bytes.
                auto files = get_overlapping_inputs(
                  level + 2_level, begin, end);
                size_t sum = total_file_size(files);
                if (sum > _vset->_options->max_grandparent_overlap_bytes()) {
                    break;
                }
            }
            ++level;
        }
    }
    return level;
}

std::unique_ptr<internal::iterator>
version::create_concatenating_iterator(internal::level level) {
    auto index_iter = std::make_unique<level_file_num_iterator>(&_files[level]);
    // Keep a strong reference at least in the lambda.
    return internal::create_two_level_iterator(
      std::move(index_iter), [self = shared_from_this()](iobuf value) {
          // We always know it's a single fragment due to how we allocate and
          // write it in the `level_file_num_iterator`.
          const auto& fragment = *value.begin();
          auto it = fragment.get();
          internal::file_id id;
          std::memcpy(&id, it, sizeof(id));
          uint64_t file_size = 0;
          std::advance(it, sizeof(id));
          std::memcpy(&file_size, it, sizeof(file_size));
          return self->_vset->_table_cache->create_iterator(id, file_size);
      });
}

ss::future<> version::for_each_overlapping(
  internal::key_view target,
  absl::FunctionRef<ss::future<ss::stop_iteration>(
    internal::level, ss::lw_shared_ptr<file_meta_data>)> fn) {
    // Search level-0 from newest to oldest
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> tmp;
    tmp.reserve(_files[0_level].size());
    for (const auto& file : _files[0_level]) {
        if (target >= file->smallest && target <= file->largest) {
            tmp.push_back(file);
        }
    }
    if (!tmp.empty()) {
        std::ranges::sort(
          tmp,
          [](
            const ss::lw_shared_ptr<file_meta_data>& a,
            const ss::lw_shared_ptr<file_meta_data>& b) {
              return a->id > b->id;
          });
        for (const auto& file : tmp) {
            auto stop = co_await fn(0_level, file);
            if (stop == ss::stop_iteration::yes) {
                co_return;
            }
        }
    }
    // Search other levels.
    for (auto level = 1_level; level() < _files.size(); ++level) {
        const auto& files = _files[level];
        if (files.empty()) {
            continue;
        }
        size_t index = find_file(files, target);
        if (index < files.size()) {
            const auto& file = files[index];
            if (target < file->smallest) {
                // All of file is past any data for the key
            } else {
                auto stop = co_await fn(level, file);
                if (stop == ss::stop_iteration::yes) {
                    co_return;
                }
            }
        }
    }
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
  ss::lw_shared_ptr<internal::options> opts)
  : _persistence(persistence)
  , _table_cache(table_cache)
  , _options(std::move(opts))
  , _compact_pointer(_options->levels.size()) {
    set_current(ss::make_lw_shared<version>(version::ctor{}, this));
}

void version_set::reuse_file_id(internal::file_id id) {
    if (id + 1_file_id == _next_file_id) {
        _next_file_id = id;
    }
}

void version_set::set_current(ss::lw_shared_ptr<version> new_version) {
    weak_intrusive_list<version>::push_front(&_current, std::move(new_version));
}

ss::future<> version_set::log_and_apply(version_edit edit) {
    auto v = ss::make_lw_shared<version>(version::ctor{}, this);
    {
        version_set::builder builder(this, _current);
        builder.apply(edit);
        builder.save_to(v.get());
    }
    finalize(v.get());
    // This is where we diverge a bit from LevelDB. We don't log manifest
    // deltas, but just snapshot the full manifest. At somepoint we will
    // want delta writes (but that's not possible in the cloud), but for
    // now we will just write full snapshots.
    auto manifest_id = new_file_id();
    auto manifest_filename = internal::manifest_file_name(manifest_id);
    auto file = co_await _persistence->open_sequential_writer(
      manifest_filename);
    auto updated_seqno = std::max(_last_seqno, edit._last_seqno);
    auto m = manifest{
      .version = v,
      .next_file_id = _next_file_id,
      .last_seqno = updated_seqno,
    };
    auto fut = co_await ss::coroutine::as_future<>(
      write_manifest(std::move(m), file.get()));
    co_await file->close();
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_await _persistence->remove_file(manifest_filename);
        reuse_file_id(manifest_id);
        std::rethrow_exception(ex);
    }
    co_await _persistence->write_file_atomically(
      internal::current_file_name(), manifest_filename);
    // Now that the new version is persisted successfully, install the new
    // version
    set_current(std::move(v));
    _last_seqno = updated_seqno;
    _current_manifest_id = manifest_id;
}

ss::future<> version_set::recover() {
    auto current = co_await read_current_file(_persistence);
    if (!current) {
        co_return;
    }
    auto maybe_parsed = internal::parse_filename(*current);
    if (!maybe_parsed) {
        throw corruption_exception(
          "unrecognized manifest file name: {}", *current);
    }
    auto manifest_file_id = maybe_parsed->id;
    auto maybe_file = co_await _persistence->open_sequential_reader(*current);
    if (!maybe_file) {
        throw corruption_exception(
          "missing current manifest file: {}", *current);
    }
    auto fut = co_await ss::coroutine::as_future<manifest>(
      read_manifest(maybe_file->get()));
    co_await (*maybe_file)->close();
    auto m = std::move(fut.get());
    finalize(m.version.get());
    set_current(std::move(m.version));
    _next_file_id = m.next_file_id;
    _last_seqno = m.last_seqno;
    _current_manifest_id = manifest_file_id;
}

void version_set::finalize(version* v) {
    // Precompute the best level for the next compaction
    internal::level best_level = 0_level;
    double best_score = static_cast<double>(v->_files[0_level].size())
                        / static_cast<double>(
                          _options->default_level_one_compaction_trigger);
    // While level 0 compaction score is based on number of files, other levels
    // are based on number of bytes in the level.
    for (auto level = 1_level; level < _options->max_level(); ++level) {
        size_t level_bytes = total_file_size(v->_files[level]);
        double score = static_cast<double>(level_bytes)
                       / static_cast<double>(max_bytes_for_level(level));
        if (score > best_score) {
            best_level = level;
            best_score = score;
        }
    }
    v->_compaction_level = best_level;
    v->_compaction_score = best_score;
}

ss::future<>
version_set::write_manifest(manifest m, io::sequential_file_writer* w) {
    proto::version version_proto;
    for (const auto& [level, files] :
         std::views::zip(std::views::iota(0), m.version->_files)) {
        proto::version_level level_proto;
        level_proto.set_number(level);
        for (const auto& file : files) {
            proto::file_meta_data file_proto;
            file_proto.set_id(file->id());
            file_proto.set_file_size(file->file_size);
            file_proto.set_encoded_smallest_key(iobuf(file->smallest));
            file_proto.set_encoded_largest_key(iobuf(file->largest));
            file_proto.set_oldest_seqno(file->oldest_seqno());
            file_proto.set_newest_seqno(file->newest_seqno());
            level_proto.get_files().push_back(std::move(file_proto));
        }
        version_proto.get_levels().push_back(std::move(level_proto));
    }
    proto::manifest manifest_proto;
    manifest_proto.set_version(std::move(version_proto));
    manifest_proto.set_next_file_id(m.next_file_id());
    manifest_proto.set_last_seqno(m.last_seqno());
    auto serialized = co_await manifest_proto.to_proto();
    co_await w->append(std::move(serialized));
}

ss::future<version_set::manifest>
version_set::read_manifest(io::sequential_file_reader* r) {
    iobuf proto;
    static constexpr size_t buffer_size = 4_KiB;
    bool done = false;
    while (!done) {
        auto buf = co_await r->read(buffer_size);
        done = buf.size_bytes() < buffer_size;
        proto.append(std::move(buf));
    }
    proto::manifest manifest_proto;
    try {
        manifest_proto = co_await proto::manifest::from_proto(std::move(proto));
    } catch (const std::exception& ex) {
        throw corruption_exception(
          "unable to parse manifest file: {}", ex.what());
    }
    auto v = ss::make_lw_shared<version>(version::ctor{}, this);
    for (const auto& level_proto : manifest_proto.get_version().get_levels()) {
        auto& files = v->_files[internal::level{
          static_cast<uint8_t>(level_proto.get_number())}];
        for (const auto& file_proto : level_proto.get_files()) {
            auto meta = ss::make_lw_shared<file_meta_data>();
            meta->id = internal::file_id{file_proto.get_id()};
            meta->file_size = file_proto.get_file_size();
            meta->smallest = internal::key(
              file_proto.get_encoded_smallest_key());
            meta->largest = internal::key(file_proto.get_encoded_largest_key());
            meta->oldest_seqno = internal::sequence_number(
              file_proto.get_oldest_seqno());
            meta->newest_seqno = internal::sequence_number(
              file_proto.get_newest_seqno());
            files.push_back(std::move(meta));
        }
    }
    manifest m;
    m.version = std::move(v);
    m.next_file_id = internal::file_id(manifest_proto.get_next_file_id());
    m.last_seqno = internal::sequence_number(manifest_proto.get_last_seqno());
    co_return m;
}

chunked_hash_set<internal::file_id> version_set::get_live_files() {
    chunked_hash_set<internal::file_id> all_files;
    for (auto v = _current; v != nullptr; v = *v->next()) {
        for (const auto& files : v->_files) {
            for (const auto& file : files) {
                all_files.insert(file->id);
            }
        }
    }
    return all_files;
}

} // namespace lsm::db
