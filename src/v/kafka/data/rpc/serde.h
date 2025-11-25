/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/format_to.h"
#include "cluster/errc.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"
#include "serde/rw/variant.h"

#include <seastar/core/chunked_fifo.hh>

namespace kafka::data::rpc {

struct kafka_topic_data
  : serde::
      envelope<kafka_topic_data, serde::version<0>, serde::compat_version<0>> {
    kafka_topic_data() = default;
    kafka_topic_data(model::topic_partition, model::record_batch);
    kafka_topic_data(
      model::topic_partition, ss::chunked_fifo<model::record_batch>);

    model::topic_partition tp;
    ss::chunked_fifo<model::record_batch> batches;

    kafka_topic_data share();

    auto serde_fields() { return std::tie(tp, batches); }

    fmt::iterator format_to(fmt::iterator it) const;
};

struct produce_request
  : serde::
      envelope<produce_request, serde::version<0>, serde::compat_version<0>> {
    produce_request() = default;
    produce_request(
      ss::chunked_fifo<kafka_topic_data> topic_data,
      model::timeout_clock::duration timeout)
      : topic_data{std::move(topic_data)}
      , timeout{timeout} {}

    auto serde_fields() { return std::tie(topic_data, timeout); }

    produce_request share();

    ss::chunked_fifo<kafka_topic_data> topic_data;
    model::timeout_clock::duration timeout{};

    fmt::iterator format_to(fmt::iterator it) const;
};

struct kafka_topic_data_result
  : serde::envelope<
      kafka_topic_data_result,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka_topic_data_result() = default;
    kafka_topic_data_result(model::topic_partition tp, cluster::errc ec)
      : tp(std::move(tp))
      , err(ec) {}

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};

    auto serde_fields() { return std::tie(tp, err); }

    fmt::iterator format_to(fmt::iterator it) const;
};

struct produce_reply
  : serde::
      envelope<produce_reply, serde::version<0>, serde::compat_version<0>> {
    produce_reply() = default;
    explicit produce_reply(ss::chunked_fifo<kafka_topic_data_result> r)
      : results(std::move(r)) {}

    auto serde_fields() { return std::tie(results); }

    ss::chunked_fifo<kafka_topic_data_result> results;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct topic_partitions
  : serde::
      envelope<topic_partitions, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(topic, partitions); }

    topic_partitions copy() const {
        return topic_partitions{
          .topic = topic, .partitions = partitions.copy()};
    }

    model::topic topic;
    chunked_vector<model::partition_id> partitions;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct partition_offsets
  : serde::
      envelope<partition_offsets, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(high_watermark, last_stable_offset); }

    kafka::offset high_watermark;
    kafka::offset last_stable_offset;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct partition_offset_result
  : serde::envelope<
      partition_offset_result,
      serde::version<0>,
      serde::compat_version<0>> {
    partition_offset_result() = default;

    explicit partition_offset_result(cluster::errc err)
      : err(err) {}

    explicit partition_offset_result(partition_offsets offsets)
      : offsets(offsets) {}

    auto serde_fields() { return std::tie(err, offsets); }

    cluster::errc err{cluster::errc::success};
    partition_offsets offsets;

    fmt::iterator format_to(fmt::iterator it) const;
};
using partition_offsets_map = chunked_hash_map<
  model::topic,
  chunked_hash_map<model::partition_id, partition_offset_result>>;

struct get_offsets_request
  : serde::envelope<
      get_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    get_offsets_request() = default;

    explicit get_offsets_request(chunked_vector<topic_partitions> topics)
      : topics(std::move(topics)) {}

    auto serde_fields() { return std::tie(topics); }

    chunked_vector<topic_partitions> topics;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct get_offsets_reply
  : serde::
      envelope<get_offsets_reply, serde::version<0>, serde::compat_version<0>> {
    get_offsets_reply() = default;

    explicit get_offsets_reply(partition_offsets_map partition_offsets)
      : partition_offsets(std::move(partition_offsets)) {}

    auto serde_fields() { return std::tie(partition_offsets); }

    partition_offsets_map partition_offsets;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct consume_request
  : serde::
      envelope<consume_request, serde::version<0>, serde::compat_version<0>> {
    consume_request() = default;
    consume_request(
      model::topic_partition tp,
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      model::timeout_clock::duration timeout)
      : tp(std::move(tp))
      , start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , timeout(timeout) {}

    auto serde_fields() {
        return std::tie(
          tp, start_offset, max_offset, min_bytes, max_bytes, timeout);
    }

    model::topic_partition tp;
    kafka::offset start_offset;
    kafka::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;
    model::timeout_clock::duration timeout{};

    fmt::iterator format_to(fmt::iterator it) const;
};

struct consume_reply
  : serde::
      envelope<consume_reply, serde::version<0>, serde::compat_version<0>> {
    consume_reply() = default;
    consume_reply(
      model::topic_partition tp,
      cluster::errc err,
      chunked_vector<model::record_batch> batches)
      : tp(std::move(tp))
      , err(err)
      , batches(std::move(batches)) {}

    auto serde_fields() { return std::tie(tp, err, batches); }

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};
    chunked_vector<model::record_batch> batches;

    fmt::iterator format_to(fmt::iterator it) const;
};

// KVStore types

struct kv_entry
  : serde::envelope<kv_entry, serde::version<0>, serde::compat_version<0>> {
    kv_entry() = default;
    kv_entry(ss::sstring key, iobuf value)
      : key(std::move(key))
      , value(std::move(value)) {}

    auto serde_fields() { return std::tie(key, value); }

    ss::sstring key;
    iobuf value;
};

struct kv_precondition_if_exists
  : serde::envelope<
      kv_precondition_if_exists,
      serde::version<0>,
      serde::compat_version<0>> {
    kv_precondition_if_exists() = default;
    explicit kv_precondition_if_exists(bool exists)
      : exists(exists) {}

    auto serde_fields() { return std::tie(exists); }

    bool exists{false};
};

struct kv_precondition_if_matches
  : serde::envelope<
      kv_precondition_if_matches,
      serde::version<0>,
      serde::compat_version<0>> {
    kv_precondition_if_matches() = default;
    explicit kv_precondition_if_matches(ss::sstring sha256_hash)
      : sha256_hash(std::move(sha256_hash)) {}

    auto serde_fields() { return std::tie(sha256_hash); }

    ss::sstring sha256_hash;
};

struct kv_no_precondition
  : serde::envelope<
      kv_no_precondition,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(); }
};

using kv_precondition = serde::variant<
  kv_no_precondition,
  kv_precondition_if_exists,
  kv_precondition_if_matches>;

struct kv_put
  : serde::envelope<kv_put, serde::version<0>, serde::compat_version<0>> {
    kv_put() = default;
    kv_put(kv_entry entry, kv_precondition precondition)
      : entry(std::move(entry))
      , precondition(std::move(precondition)) {}

    auto serde_fields() { return std::tie(entry, precondition); }

    kv_entry entry;
    kv_precondition precondition;
};

struct kv_remove
  : serde::envelope<kv_remove, serde::version<0>, serde::compat_version<0>> {
    kv_remove() = default;
    kv_remove(ss::sstring key, kv_precondition precondition)
      : key(std::move(key))
      , precondition(std::move(precondition)) {}

    auto serde_fields() { return std::tie(key, precondition); }

    ss::sstring key;
    kv_precondition precondition;
};

struct kv_write_request
  : serde::
      envelope<kv_write_request, serde::version<0>, serde::compat_version<0>> {
    kv_write_request() = default;
    kv_write_request(
      model::ntp ntp,
      chunked_vector<kv_put> puts,
      chunked_vector<kv_remove> removals)
      : ntp(std::move(ntp))
      , puts(std::move(puts))
      , removals(std::move(removals)) {}

    auto serde_fields() { return std::tie(ntp, puts, removals); }

    model::ntp ntp;
    chunked_vector<kv_put> puts;
    chunked_vector<kv_remove> removals;
};

struct kv_write_reply
  : serde::
      envelope<kv_write_reply, serde::version<0>, serde::compat_version<0>> {
    kv_write_reply() = default;

    explicit kv_write_reply(cluster::errc err)
      : err(err) {}

    auto serde_fields() { return std::tie(err); }

    cluster::errc err{cluster::errc::success};
};

struct kv_get_request
  : serde::
      envelope<kv_get_request, serde::version<0>, serde::compat_version<0>> {
    kv_get_request() = default;
    kv_get_request(model::ntp ntp, chunked_vector<ss::sstring> keys)
      : ntp(std::move(ntp))
      , keys(std::move(keys)) {}

    auto serde_fields() { return std::tie(ntp, keys); }

    model::ntp ntp;
    chunked_vector<ss::sstring> keys;
};

struct kv_get_result
  : serde::
      envelope<kv_get_result, serde::version<0>, serde::compat_version<0>> {
    kv_get_result() = default;
    explicit kv_get_result(ss::sstring key)
      : key(std::move(key)) {}

    kv_get_result(ss::sstring key, std::optional<iobuf> value)
      : key(std::move(key))
      , value(std::move(value)) {}

    auto serde_fields() { return std::tie(key, value); }

    ss::sstring key;
    std::optional<iobuf> value;
};

struct kv_get_reply
  : serde::envelope<kv_get_reply, serde::version<0>, serde::compat_version<0>> {
    kv_get_reply() = default;
    explicit kv_get_reply(chunked_vector<kv_get_result> results)
      : results(std::move(results)) {}

    explicit kv_get_reply(cluster::errc err)
      : err(err) {}

    auto serde_fields() { return std::tie(results, err); }

    chunked_vector<kv_get_result> results;
    cluster::errc err{cluster::errc::success};
};

struct kv_scan_request
  : serde::
      envelope<kv_scan_request, serde::version<0>, serde::compat_version<0>> {
    kv_scan_request() = default;
    kv_scan_request(
      model::ntp ntp,
      std::optional<ss::sstring> start_key,
      std::optional<ss::sstring> end_key,
      uint32_t limit)
      : ntp(std::move(ntp))
      , start_key(std::move(start_key))
      , end_key(std::move(end_key))
      , limit(limit) {}

    auto serde_fields() { return std::tie(ntp, start_key, end_key, limit); }

    model::ntp ntp;
    std::optional<ss::sstring> start_key;
    std::optional<ss::sstring> end_key;
    uint32_t limit{0};
};

struct kv_scan_reply
  : serde::
      envelope<kv_scan_reply, serde::version<0>, serde::compat_version<0>> {
    kv_scan_reply() = default;
    kv_scan_reply(cluster::errc err, chunked_vector<kv_entry> entries)
      : err(err)
      , entries(std::move(entries)) {}

    explicit kv_scan_reply(cluster::errc err)
      : err(err) {}

    auto serde_fields() { return std::tie(err, entries); }

    cluster::errc err{cluster::errc::success};
    chunked_vector<kv_entry> entries;
};

} // namespace kafka::data::rpc
