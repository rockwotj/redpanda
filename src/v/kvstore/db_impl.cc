/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kvstore/db_impl.h"

#include "cloud_storage_clients/types.h"
#include "crypto/crypto.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/utils/txn_reader.h"
#include "kvstore/logger.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/lsm.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
#include <iterator>
#include <stdexcept>
#include <utility>

namespace kvstore {

constexpr int32_t max_key_size = 16_KiB;
constexpr int32_t max_batch_size = 1_MiB;

namespace {

bool hash_matches(const iobuf& b, std::string_view hex_hash) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    for (const auto& f : b) {
        ctx.update({f.get(), f.size()});
    }
    auto digest = ctx.reset();
    return to_hex(digest) == hex_hash;
}

// Encode a key such that null characters are removed
void encode_key(std::string_view s, std::string* out) {
    for (char c : s) {
        if (c == '\0') {
            out->append("\1\1");
        } else if (c == '\1') {
            out->append("\1\2");
        } else {
            out->append(1, c);
        }
    }
}

std::string encode_key(const iobuf& b) {
    std::string o;
    for (const auto& frag : b) {
        encode_key(std::string_view{frag}, &o);
    }
    return o;
}
std::string encode_key(std::string_view s) {
    std::string o;
    encode_key(s, &o);
    return o;
}
std::string encode_key(const ss::sstring& s) {
    return encode_key(std::string_view(s));
}

// Unescape our encoded key
ss::sstring decode_key(std::string_view s) {
    ss::sstring decoded{ss::sstring::initialized_later{}, s.size()};
    auto out = decoded.begin();
    for (auto it = s.begin(); it != s.end(); std::advance(it, 1)) {
        if (*it == '\1') {
            std::advance(it, 1);
            *out = *it == '\1' ? '\0' : '\1';
        } else {
            *out = *it;
        }
    }
    decoded.erase(out, decoded.end());
    return decoded;
}

} // namespace

std::unique_ptr<db> db::make(
  ss::lw_shared_ptr<cluster::partition> partition,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  std::filesystem::path staging_dir) {
    return std::make_unique<db_impl>(
      std::move(partition),
      remote,
      std::move(bucket),
      std::move(prefix),
      std::move(staging_dir));
}

ss::future<> db_impl::start() {
    vlog(kvlog.info, "starting kvstore db for {}", _partition->ntp());
    if (!_partition->is_leader()) {
        co_return;
    }
    _term = _partition->term();
    auto data = co_await lsm::io::open_cloud_data_persistence(
      _staging_dir, _remote, _bucket, _prefix);
    auto metadata = co_await lsm::io::open_cloud_metadata_persistence(
      _remote, _bucket, _prefix);
    _lsm = co_await lsm::database::open(
      {

      },
      lsm::io::persistence{
        .data = std::move(data),
        .metadata = std::move(metadata),
      });
    _last_applied_offset = _lsm->max_applied_offset()
                             .transform([](auto seqno) {
                                 return model::offset(
                                   static_cast<int64_t>(seqno));
                             })
                             .value_or(model::offset::min());
    ssx::spawn_with_gate(_gate, [this] { return apply_loop(); });
    vlog(
      kvlog.info,
      "started kvstore db for {} with last applied offset {}",
      _partition->ntp(),
      _last_applied_offset);
}

ss::future<> db_impl::stop() {
    vlog(kvlog.info, "stopping kvstore db for {}", _partition->ntp());
    _as.request_abort();
    co_await _gate.close();
    if (auto lsm = std::exchange(_lsm, std::nullopt)) {
        co_await lsm->close();
    }
    vlog(kvlog.info, "stopped kvstore db for {}", _partition->ntp());
}

ss::future<> db::destroy(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  ss::abort_source& as) {
    retry_chain_node root{as};
    auto list_result = co_await remote->list_objects(bucket, root, prefix);
    if (!list_result) {
        throw std::runtime_error(
          fmt::format(
            "unable to list database for destruction: {}",
            list_result.error()));
    }
    chunked_vector<cloud_storage_clients::object_key> keys;
    for (const auto& item : list_result.value().contents) {
        keys.emplace_back(item.key);
    }
    auto delete_result = co_await remote->delete_objects(
      bucket, std::move(keys), root, [](size_t) {});
    switch (delete_result) {
    case cloud_io::upload_result::success:
        co_return;
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::failed:
    case cloud_io::upload_result::cancelled:
        throw std::runtime_error(
          fmt::format("unable to destroy database: {}", delete_result));
    }
}

ss::future<chunked_vector<std::optional<iobuf>>>
db_impl::batch_get(const chunked_vector<ss::sstring>& keys) {
    auto _ = _gate.hold();
    co_await sync_previous_term();
    if (!_lsm) {
        throw ss::abort_requested_exception();
    }
    chunked_vector<std::optional<iobuf>> values;
    for (const auto& key : keys) {
        values.push_back(co_await _lsm->get(encode_key(key)));
    }
    co_return values;
}

ss::future<chunked_vector<entry>> db_impl::scan(scan_parameters params) {
    auto _ = _gate.hold();
    if (!_lsm) {
        throw ss::abort_requested_exception();
    }
    co_await sync_previous_term();
    chunked_vector<entry> result;
    auto iter = co_await _lsm->create_iterator();
    if (const auto& start = params.start_key) {
        co_await iter.seek(encode_key(*start));
    } else {
        co_await iter.seek_to_first();
        vlog(
          kvlog.debug,
          "scanning at most {} results starting valid after seek to first {}, "
          "last applied: {}",
          params.limit,
          iter.valid(),
          _lsm->max_applied_offset());
    }
    if (const auto& end = params.end_key) {
        auto stop = encode_key(*end);
        for (size_t i = 0;
             i < params.limit && iter.valid() && iter.key() < stop;
             ++i, co_await iter.next()) {
            _as.check();
            result.emplace_back(decode_key(iter.key()), iter.value());
        }
    } else {
        for (size_t i = 0; i < params.limit && iter.valid();
             ++i, co_await iter.next()) {
            _as.check();
            result.emplace_back(decode_key(iter.key()), iter.value());
        }
    }
    vlog(kvlog.debug, "scan returned {} results", result.size());
    co_return result;
}

ss::future<write_success> db_impl::write(write_batch wb) {
    auto h = _gate.hold();
    if (!_lsm) {
        throw ss::abort_requested_exception();
    }
    auto _ = co_await _write_mu.get_units();
    co_await sync_latest();
    model::batch_builder bb;
    for (auto& op : wb.puts) {
        _as.check();
        auto success = co_await check_precondition(
          op.entry.key, op.precondition);
        if (!success) {
            co_return success;
        }
        bb.add_record(
          model::record(
            /*attributes=*/{},
            /*timestamp_delta=*/0,
            /*offset_delta=*/bb.num_records(),
            /*key=*/iobuf::from(op.entry.key),
            /*value=*/std::move(op.entry.value),
            /*hdrs=*/{}));
    }
    for (auto& op : wb.removals) {
        _as.check();
        auto success = co_await check_precondition(op.key, op.precondition);
        if (!success) {
            co_return success;
        }
        bb.add_record(
          model::record(
            /*attributes=*/{},
            /*timestamp_delta=*/0,
            /*offset_delta=*/bb.num_records(),
            /*key=*/iobuf::from(op.key),
            /*value=*/std::nullopt,
            /*hdrs=*/{}));
    }
    auto batch = co_await std::move(bb).build();
    co_await replicate(std::move(batch));
    co_await sync_latest();
    co_return write_success::yes;
}

ss::future<write_success>
db_impl::check_precondition(std::string_view key, const precondition& p) {
    bool ok = co_await ss::visit(
      p,
      [](const std::nullopt_t&) -> ss::future<bool> {
          return ss::as_ready_future(true);
      },
      [this, key](const if_exists& e) -> ss::future<bool> {
          return _lsm->get(encode_key(key)).then([e](std::optional<iobuf> v) {
              return v.has_value() == e.exists;
          });
      },
      [this, key](const if_matches& m) -> ss::future<bool> {
          return _lsm->get(encode_key(key)).then([m](std::optional<iobuf> v) {
              return v.has_value() && hash_matches(v.value(), m.sha256_hash);
          });
      });
    co_return write_success(ok);
}

ss::future<> db_impl::sync_latest() {
    auto proxy = kafka::make_partition_proxy(_partition);
    auto lso = proxy.last_stable_offset();
    if (!lso) {
        throw std::runtime_error("unable to determine lso");
    }
    co_await _cond_var.wait(
      _as, [this, lso = lso.value()] { return _last_applied_offset < lso; });
}

ss::future<> db_impl::sync_previous_term() {
    auto proxy = kafka::make_partition_proxy(_partition);
    if (!proxy.is_leader()) {
        co_return;
    }
    auto epoch = proxy.leader_epoch();
    if (epoch == kafka::invalid_leader_epoch) {
        co_return;
    }
    auto last_term_offset = co_await proxy.get_leader_epoch_last_offset(epoch);
    if (!last_term_offset) {
        co_return;
    }
    co_await _cond_var.wait(_as, [this, o = last_term_offset.value()] {
        return _last_applied_offset < o;
    });
}

ss::future<> db_impl::replicate(model::record_batch b) {
    // TODO: Check the actual max batch size
    if (b.size_bytes() > max_batch_size) {
        throw std::invalid_argument(
          fmt::format(
            "max batch size is {} got batch size {}",
            max_batch_size,
            b.size_bytes()));
    }
    raft::replicate_options opts(
      raft::consistency_level::quorum_ack,
      _term,
      /*timeout=*/std::nullopt,
      /*as=*/_as);
    auto result = co_await _partition->replicate(
      chunked_vector<model::record_batch>::single(std::move(b)), opts);
    if (result.has_error()) {
        throw std::runtime_error(
          fmt::format("unable to replicate write: {}", result.error()));
    }
}

ss::future<> db_impl::apply_loop() {
    auto& monitor = _partition->raft()->visible_offset_monitor();
    while (!_as.abort_requested()) {
        // grab the hwm because we don't want to re-apply until this has
        // advanced. However we only actually apply up to the LSO.
        auto hwm = _partition->high_watermark();
        auto fut = co_await ss::coroutine::as_future(do_apply_chunk());
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(kvlog.error, "unable to apply records to database: {}", ex);
            co_await ss::sleep_abortable(1s, _as).handle_exception(
              [](const std::exception_ptr&) {});
            continue;
        }
        // Wait for the next offset to be available before re-attempting
        // to apply writes to the database
        co_await monitor.wait(hwm, model::no_timeout, _as);
    }
}

ss::future<> db_impl::do_apply_chunk() {
    auto proxy = kafka::make_partition_proxy(_partition);
    // TODO: Add an STM to ensure the log isn't deleted until the offset is
    // applied.
    auto start_offset = model::offset_cast(
      std::max(proxy.start_offset(), model::next_offset(_last_applied_offset)));
    auto maybe_lso = proxy.last_stable_offset();
    if (!maybe_lso) {
        throw std::runtime_error("unable to determine lso");
    }

    kafka::offset max_offset = model::offset_cast(
      model::prev_offset(maybe_lso.value()));
    vlog(
      kvlog.trace,
      "creating reader to apply to kvstore, start_offset={}, max_offset={}, "
      "lso={}, hwm={}, last_applied={}",
      start_offset,
      max_offset,
      maybe_lso.value(),
      proxy.high_watermark(),
      _last_applied_offset);
    if (start_offset > max_offset) {
        co_return;
    }
    auto translator = co_await proxy.make_reader(
      {start_offset, max_offset, _as});
    auto tracker = kafka::aborted_transaction_tracker::create_default(
      &proxy, std::move(translator.ot_state));
    auto generator
      = model::make_record_batch_reader<kafka::read_committed_reader>(
          std::move(tracker), std::move(translator.reader))
          .generator(model::no_timeout);
    while (auto batch = co_await generator()) {
        if (batch->compressed()) {
            batch = co_await model::decompress_batch(*batch);
        }
        auto wb = _lsm->create_write_batch();
        auto it = model::record_batch_iterator::create(*batch);
        int applied_count = 0;
        while (it.has_next()) {
            auto record = it.next();
            if (!record.has_key() || record.key_size() > max_key_size) {
                continue;
            }
            ++applied_count;
            auto offset = batch->base_offset()
                          + model::offset_delta(record.offset_delta());
            if (record.is_tombstone()) {
                wb.remove(encode_key(record.key()), offset);
            } else {
                wb.put(
                  encode_key(record.key()), record.release_value(), offset);
            }
        }
        co_await _lsm->apply(std::move(wb));
        _last_applied_offset = batch->last_offset();
        vlog(
          kvlog.trace,
          "applied {} records to the kvstore, last applied: {}",
          applied_count,
          _last_applied_offset);
        _cond_var.broadcast();
    }
}

} // namespace kvstore
