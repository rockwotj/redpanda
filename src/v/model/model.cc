// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "model/validation.h"
#include "serde/rw/enum.h"
#include "serde/rw/rw.h"
#include "serde/rw/sstring.h"
#include "serde/serde_exception.h"
#include "strings/string_switch.h"
#include "utils/base64.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <fmt/ostream.h>

#include <iostream>
#include <optional>
#include <type_traits>

namespace model {

void read_nested(
  iobuf_parser& in, timestamp& ts, const size_t bytes_left_limit) {
    serde::read_nested(in, ts._v, bytes_left_limit);
}

void write_nested(iobuf& out, timestamp ts) { serde::write(out, ts._v); }

fmt::iterator topic_partition_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_id_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic_id, partition());
}

fmt::iterator ntp::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}/{}}}", ns(), tp.topic(), tp.partition());
}

fmt::iterator topic_namespace::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

fmt::iterator topic_namespace_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

std::ostream& operator<<(std::ostream& os, timestamp_type ts) {
    return os << format_as(ts);
}

fmt::iterator record_header::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{key_size={}, key={}, value_size={}, value={}}}",
      _key_size,
      fmt::streamed(_key),
      _val_size,
      fmt::streamed(_value));
}

fmt::iterator record::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{record: size_bytes={}, attributes={}, timestamp_delta={}, "
      "offset_delta={}, key_size={}, key={}, value_size={}, value={}, "
      "header_size:{}, headers=[",
      _size_bytes,
      _attributes,
      _timestamp_delta,
      _offset_delta,
      _key_size,
      fmt::streamed(_key),
      _val_size,
      fmt::streamed(_value),
      _headers.size());
    for (const auto& h : _headers) {
        it = h.format_to(it);
    }
    return fmt::format_to(it, "]}}");
}

fmt::iterator producer_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{producer_identity: id={}, epoch={}}}", id, epoch);
}

fmt::iterator record_batch_attributes::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{compression:");
    if (is_valid_compression()) {
        it = fmt::format_to(it, "{}", compression());
    } else {
        it = fmt::format_to(it, "invalid compression");
    }
    return fmt::format_to(
      it,
      ", type:{}, transactional: {}, control: {}}}",
      timestamp_type(),
      is_transactional(),
      is_control());
}

fmt::iterator record_batch_header::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{header_crc:{}, size_bytes:{}, base_offset:{}, type:{}, crc:{}, "
      "attrs:{}, last_offset_delta:{}, first_timestamp:{}, max_timestamp:{}, "
      "producer_id:{}, producer_epoch:{}, base_sequence:{}, record_count:{}, "
      "ctx:{{term:{}, owner_shard:",
      header_crc,
      size_bytes,
      base_offset,
      type,
      crc,
      attrs,
      last_offset_delta,
      first_timestamp,
      max_timestamp,
      producer_id,
      producer_epoch,
      base_sequence,
      record_count,
      ctx.term);
    if (ctx.owner_shard) {
        it = fmt::format_to(it, "{}}}", *ctx.owner_shard);
    } else {
        it = fmt::format_to(it, "nullopt}}");
    }
    return fmt::format_to(it, "}}");
}

fmt::iterator record_batch::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{record_batch={}, records=", _header);
    if (_compressed) {
        it = fmt::format_to(
          it, "{{compressed={} bytes}}", _records.size_bytes());
    } else {
        it = fmt::format_to(it, "{{");
        for_each_record(
          [&it](const model::record& r) { it = r.format_to(it); });
        it = fmt::format_to(it, "}}");
    }
    return fmt::format_to(it, "}}");
}

ss::sstring ntp::path() const {
    return ssx::sformat("{}/{}/{}", ns(), tp.topic(), tp.partition());
}

ss::sstring topic_namespace::path() const {
    return ssx::sformat("{}/{}", ns(), tp());
}

std::filesystem::path ntp::topic_path() const {
    return fmt::format("{}/{}", ns(), tp.topic());
}

std::istream& operator>>(std::istream& i, compression& c) {
    ss::sstring s;
    i >> s;
    auto tmp = string_switch<std::optional<compression>>(s)
                 .match_all("none", "uncompressed", compression::none)
                 .match("gzip", compression::gzip)
                 .match("snappy", compression::snappy)
                 .match("lz4", compression::lz4)
                 .match("zstd", compression::zstd)
                 .match("producer", compression::producer)
                 .default_match(std::nullopt);

    if (tmp.has_value()) {
        c = tmp.value();
    } else {
        i.setstate(std::ios_base::failbit);
    }

    return i;
}

fmt::iterator broker_properties::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{cores {}, mem_available {}, disk_available {}, in_fips_mode {}}}",
      cores,
      available_memory_bytes,
      available_disk_gb,
      in_fips_mode);
}

fmt::iterator broker_endpoint::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}:{}}}", name, address);
}

fmt::iterator broker::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, kafka_advertised_listeners: {}, rpc_address: {}, rack: {}, "
      "properties: {}}}",
      _id,
      _kafka_advertised_listeners,
      _rpc_address,
      _rack,
      _properties);
}

fmt::iterator topic_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic_namespace: {}, partitons: {}}}", tp_ns, partitions);
}

fmt::iterator partition_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, leader_id: {}, replicas: {}}}",
      id,
      leader_node,
      replicas);
}

fmt::iterator broker_shard::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{node_id: {}, shard: {}}}", node_id, shard);
}

std::ostream& operator<<(std::ostream& o, compaction_strategy c) {
    return o << format_as(c);
}

std::istream& operator>>(std::istream& i, compaction_strategy& cs) {
    ss::sstring s;
    i >> s;
    cs = string_switch<compaction_strategy>(s)
           .match("offset", compaction_strategy::offset)
           .match("header", compaction_strategy::header)
           .match("timestamp", compaction_strategy::timestamp);
    return i;
};

std::istream& operator>>(std::istream& i, timestamp_type& ts_type) {
    ss::sstring s;
    i >> s;
    ts_type = string_switch<timestamp_type>(s)
                .match("LogAppendTime", timestamp_type::append_time)
                .match("CreateTime", timestamp_type::create_time);
    return i;
};

std::ostream& operator<<(std::ostream& o, cleanup_policy_bitflags c) {
    return o << format_as(c);
}

std::istream& operator>>(std::istream& i, cleanup_policy_bitflags& cp) {
    ss::sstring s;
    i >> s;
    cp = string_switch<cleanup_policy_bitflags>(s)
           .match("delete", cleanup_policy_bitflags::deletion)
           .match("compact", cleanup_policy_bitflags::compaction)
           .match_all(
             "compact,delete",
             "delete,compact",
             cleanup_policy_bitflags::deletion
               | cleanup_policy_bitflags::compaction);
    return i;
}

std::ostream& operator<<(std::ostream& o, record_batch_type bt) {
    return o << format_as(bt);
}

std::ostream& operator<<(std::ostream& o, membership_state st) {
    return o << format_as(st);
}

std::ostream& operator<<(std::ostream& o, maintenance_state st) {
    return o << format_as(st);
}

std::ostream& operator<<(std::ostream& os, const cloud_credentials_source& cs) {
    return os << format_as(cs);
}

std::ostream& operator<<(std::ostream& o, const shadow_indexing_mode& si) {
    return o << format_as(si);
}

std::ostream& operator<<(std::ostream& o, const control_record_type& crt) {
    return o << format_as(crt);
}

fmt::iterator batch_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{pid: {}, first_seq: {}, is_transactional: {}, record_count: {}, "
      "last_seq: {}}}",
      pid,
      first_seq,
      is_transactional,
      record_count,
      last_seq);
}

std::ostream& operator<<(std::ostream& o, fetch_read_strategy s) {
    return o << format_as(s);
}

std::istream& operator>>(std::istream& i, fetch_read_strategy& strat) {
    ss::sstring s;
    i >> s;
    strat = string_switch<fetch_read_strategy>(s)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::polling),
                fetch_read_strategy::polling)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::non_polling),
                fetch_read_strategy::non_polling)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_debounce),
                fetch_read_strategy::non_polling_with_debounce)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_pid),
                fetch_read_strategy::non_polling_with_pid);
    return i;
}

std::ostream& operator<<(std::ostream& o, write_caching_mode mode) {
    return o << format_as(mode);
}

std::istream& operator>>(std::istream& i, write_caching_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = write_caching_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

std::optional<write_caching_mode>
write_caching_mode_from_string(std::string_view s) {
    return string_switch<std::optional<write_caching_mode>>(s)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_true),
        model::write_caching_mode::default_true)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_false),
        model::write_caching_mode::default_false)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::disabled),
        model::write_caching_mode::disabled)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& o, redpanda_storage_mode mode) {
    return o << format_as(mode);
}

std::istream& operator>>(std::istream& i, redpanda_storage_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = redpanda_storage_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

std::optional<redpanda_storage_mode>
redpanda_storage_mode_from_string(std::string_view s) {
    return string_switch<std::optional<redpanda_storage_mode>>(s)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::local),
        model::redpanda_storage_mode::local)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::tiered),
        model::redpanda_storage_mode::tiered)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::cloud),
        model::redpanda_storage_mode::cloud)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::unset),
        model::redpanda_storage_mode::unset)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, recovery_validation_mode vm) {
    return os << format_as(vm);
}

std::istream& operator>>(std::istream& is, recovery_validation_mode& vm) {
    using enum recovery_validation_mode;
    auto s = ss::sstring{};
    is >> s;
    try {
        vm = string_switch<recovery_validation_mode>(s)
               .match("check_manifest_existence", check_manifest_existence)
               .match(
                 "check_manifest_and_segment_metadata",
                 check_manifest_and_segment_metadata)
               .match("no_check", no_check);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

iceberg_mode iceberg_mode::disabled
  = iceberg_mode::make<iceberg_mode::variant::disabled>();
iceberg_mode iceberg_mode::key_value
  = iceberg_mode::make<iceberg_mode::variant::key_value>();
iceberg_mode iceberg_mode::value_schema_id_prefix
  = iceberg_mode::make<iceberg_mode::variant::value_schema_id_prefix>();

void write_nested(iobuf& out, const iceberg_mode& m) {
    using serde::write;
    write(out, m.kind());
    if (m.kind() == iceberg_mode::variant::value_schema_latest) {
        write(out, m.protobuf_full_name().value_or(""));
        write(out, m.subject_name().value_or(""));
    }
}

void read_nested(
  iobuf_parser& in, iceberg_mode& m, const std::size_t bytes_left_limit) {
    using serde::read_nested;
    iceberg_mode::variant v = iceberg_mode::variant::disabled;
    read_nested(in, v, bytes_left_limit);
    switch (v) {
    case iceberg_mode::variant::disabled:
        m = iceberg_mode::disabled;
        return;
    case iceberg_mode::variant::key_value:
        m = iceberg_mode::key_value;
        return;
    case iceberg_mode::variant::value_schema_id_prefix:
        m = iceberg_mode::value_schema_id_prefix;
        return;
    case iceberg_mode::variant::value_schema_latest:
        ss::sstring msg_name;
        read_nested(in, msg_name, bytes_left_limit);
        ss::sstring subject;
        read_nested(in, subject, bytes_left_limit);
        m = iceberg_mode::value_schema_latest(msg_name, subject);
        return;
    }
    throw serde::serde_exception(
      fmt::format("unknown iceberg_mode variant: {}", std::to_underlying(v)));
}

fmt::iterator iceberg_mode::format_to(fmt::iterator it) const {
    switch (kind()) {
    case variant::disabled:
        return fmt::format_to(it, "disabled");
    case variant::key_value:
        return fmt::format_to(it, "key_value");
    case variant::value_schema_id_prefix:
        return fmt::format_to(it, "value_schema_id_prefix");
    case variant::value_schema_latest:
        it = fmt::format_to(it, "value_schema_latest");
        bool delimiter = false;
        if (auto pname = protobuf_full_name()) {
            it = fmt::format_to(it, ":protobuf_name={}", pname.value());
            delimiter = true;
        }
        if (auto subj = subject_name()) {
            it = fmt::format_to(
              it, "{}subject={}", delimiter ? "," : ":", subj.value());
        }
        return it;
    }
}

std::ostream& operator<<(std::ostream& os, const iceberg_mode& mode) {
    fmt::print(os, "{}", mode);
    return os;
}

namespace {
// Parse configuration options for iceberg_mode's value_schema_latest, which
// is a grammar like: `:(<name>=<value>)+`
std::optional<absl::flat_hash_map<std::string, std::string>>
parse_config_options(std::string_view str) {
    if (str.empty()) {
        return absl::flat_hash_map<std::string, std::string>{};
    }
    if (!absl::ConsumePrefix(&str, ":")) {
        return std::nullopt;
    }
    if (str.empty()) {
        return std::nullopt;
    }
    absl::flat_hash_map<std::string, std::string> result;
    for (std::string_view pair : absl::StrSplit(str, ",")) {
        auto [it, inserted] = result.insert(
          absl::StrSplit(pair, absl::MaxSplits("=", 1)));
        // Don't allow duplicates
        if (!inserted) {
            return std::nullopt;
        }
        // Don't allow empty keys or values
        if (it->first.empty() || it->second.empty()) {
            return std::nullopt;
        }
    }
    return result;
}
} // namespace

std::istream& operator>>(std::istream& is, iceberg_mode& mode) {
    ss::sstring s;
    is >> s;
    if (s == "disabled") {
        mode = iceberg_mode::disabled;
    } else if (s == "key_value") {
        mode = iceberg_mode::key_value;
    } else if (s == "value_schema_id_prefix") {
        mode = iceberg_mode::value_schema_id_prefix;
    } else if (s.starts_with("value_schema_latest")) {
        s = s.substr(std::strlen("value_schema_latest"));
        auto options = parse_config_options(s);
        if (!options.has_value()) {
            is.setstate(std::ios::failbit);
            return is;
        }
        std::string_view protobuf_name;
        std::string_view subject;
        for (const auto& [key, value] : options.value()) {
            if (key == "protobuf_name") {
                protobuf_name = value;
            } else if (key == "subject") {
                subject = value;
            } else {
                is.setstate(std::ios::failbit);
                return is;
            }
        }
        mode = iceberg_mode::value_schema_latest(protobuf_name, subject);
    } else {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::ostream&
operator<<(std::ostream& os, const iceberg_invalid_record_action& a) {
    return os << format_as(a);
}

std::istream& operator>>(std::istream& is, iceberg_invalid_record_action& a) {
    using enum iceberg_invalid_record_action;
    ss::sstring s;
    is >> s;
    try {
        a = string_switch<iceberg_invalid_record_action>(s)
              .match("drop", drop)
              .match("dlq_table", dlq_table);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::ostream& operator<<(std::ostream& os, const fips_mode_flag& f) {
    return os << format_as(f);
}

std::istream& operator>>(std::istream& is, fips_mode_flag& f) {
    ss::sstring s;
    is >> s;
    f = string_switch<fips_mode_flag>(s)
          .match(
            to_string_view(fips_mode_flag::disabled), fips_mode_flag::disabled)
          .match(
            to_string_view(fips_mode_flag::enabled), fips_mode_flag::enabled)
          .match(
            to_string_view(fips_mode_flag::permissive),
            fips_mode_flag::permissive);
    return is;
}

fmt::iterator topic_id::format_to(fmt::iterator it) const {
    const auto& uuid = (*this)().uuid();
    const bytes_view bv{uuid.begin(), uuid.size()};
    return fmt::format_to(it, "{}", bytes_to_base64(bv));
}

topic_id_partition topic_id_partition::from(std::string_view s) {
    std::vector<ss::sstring> ss = absl::StrSplit(s, "/");
    if (ss.size() != 2) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    auto tid = uuid_t::from_string(ss[0]);
    int p{0};
    if (!absl::SimpleAtoi(ss[1].data(), &p)) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    return model::topic_id_partition(
      model::topic_id(tid), model::partition_id(p));
}

std::optional<kafka_batch_validation_mode>
kafka_batch_validation_mode_from_string(std::string_view s) {
    return string_switch<std::optional<kafka_batch_validation_mode>>(s)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::legacy),
        model::kafka_batch_validation_mode::legacy)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::relaxed),
        model::kafka_batch_validation_mode::relaxed)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::strict),
        model::kafka_batch_validation_mode::strict)
      .default_match(std::nullopt);
}

std::ostream&
operator<<(std::ostream& o, const kafka_batch_validation_mode& mode) {
    return o << format_as(mode);
}

std::istream& operator>>(std::istream& i, kafka_batch_validation_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = kafka_batch_validation_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

} // namespace model
