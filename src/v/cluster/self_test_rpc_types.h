/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "config/node_config.h"
#include "json/document.h"
#include "model/metadata.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "utils/uuid.h"

#include <seastar/core/scheduling.hh>

namespace cluster {

enum class self_test_status : int8_t { idle = 0, running, unreachable };

ss::sstring self_test_status_as_string(self_test_status sts);

ss::sstring format_as(self_test_status sts);
std::ostream& operator<<(std::ostream& o, self_test_status sts);

enum class self_test_stage : int8_t { idle = 0, disk, net, cloud };

ss::sstring self_test_stage_as_string(self_test_stage sts);

ss::sstring format_as(self_test_stage sts);
std::ostream& operator<<(std::ostream& o, self_test_stage sts);

struct diskcheck_opts
  : serde::
      envelope<diskcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name{"unspecified"};
    /// Where files this benchmark will read/write to exist
    std::filesystem::path dir{config::node().disk_benchmark_path()};
    /// Open the file with O_DSYNC flag option
    bool dsync{true};
    /// Set to true to disable the write portion of the benchmark
    bool skip_write{false};
    /// Set to true to disable the read portion of the benchmark
    bool skip_read{false};
    /// Total size of all benchmark files to exist on disk
    uint64_t data_size{10ULL << 30}; // 10GiB
    /// Size of individual read and/or write requests
    size_t request_size{512 << 10}; // 512KiB
    /// Total duration of the benchmark
    ss::lowres_clock::duration duration{std::chrono::milliseconds(5000)};
    /// Amount of fibers to run per shard
    uint16_t parallelism{10};
    /// Scheduling group that the benchmark will operate under
    ss::scheduling_group sg;

    /// Total size a single shard will write/read to a single file on disk
    uint64_t file_size() const { return data_size / parallelism; }
    /// Address where allocated memory is placed will be a multiple of this
    uint64_t alignment() const { return request_size >= 4096 ? 4096ULL : 512; }

    static diskcheck_opts from_json(const json::Value& obj) {
        /// The application using these parameters will perform any validation
        diskcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("dsync")) {
            opts.dsync = obj["dsync"].GetBool();
        }
        if (obj.HasMember("skip_write")) {
            opts.skip_write = obj["skip_write"].GetBool();
        }
        if (obj.HasMember("skip_read")) {
            opts.skip_read = obj["skip_read"].GetBool();
        }
        if (obj.HasMember("data_size")) {
            opts.data_size = obj["data_size"].GetUint64();
        }
        if (obj.HasMember("request_size")) {
            opts.request_size = obj["request_size"].GetUint64();
        }
        if (obj.HasMember("duration_ms")) {
            opts.duration = std::chrono::milliseconds(
              obj["duration_ms"].GetInt());
        }
        if (obj.HasMember("parallelism")) {
            opts.parallelism = obj["parallelism"].GetUint();
        }
        return opts;
    }

    auto serde_fields() {
        return std::tie(
          name,
          dsync,
          skip_write,
          skip_read,
          data_size,
          request_size,
          duration,
          parallelism);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{name: {} dsync: {} skip_write: {} skip_read: {} data_size: {} "
          "request_size: {} duration: {} parallelism: {}}}",
          name,
          dsync,
          skip_write,
          skip_read,
          data_size,
          request_size,
          duration,
          parallelism);
    }
};

struct netcheck_opts
  : serde::
      envelope<netcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name{"Network Test 8192b packet size"};
    /// Node ids of servers to run the test against
    std::vector<model::node_id> peers;
    /// Size of individual request
    size_t request_size{2 << 12}; // 8192 bytes
    /// Total duration of an individual benchmark
    ss::lowres_clock::duration duration{std::chrono::milliseconds(5000)};
    /// Total duration of the entire benchmark across all nodes, ensures no
    /// broker will be stuck in a blocked state for infinite time. This is
    /// automatically set to the number of network benchmarks multiplied by the
    /// above duration of a single netcheck run
    ss::lowres_clock::duration max_duration;
    /// Number of fibers per shard used to make network requests
    uint16_t parallelism{10};
    /// Scheduling group that the benchmark will operate under
    ss::scheduling_group sg;

    static netcheck_opts from_json(const json::Value& obj) {
        /// The application using these parameters will perform any validation
        netcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("request_size")) {
            opts.request_size = obj["request_size"].GetUint64();
        }
        if (obj.HasMember("duration_ms")) {
            opts.duration = std::chrono::milliseconds(
              obj["duration_ms"].GetInt());
        }
        if (obj.HasMember("parallelism")) {
            opts.parallelism = obj["parallelism"].GetInt();
        }
        return opts;
    }

    auto serde_fields() {
        return std::tie(
          name, peers, request_size, duration, max_duration, parallelism);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{name: {} peers: {} request_size: {} duration: "
          "{} max_duration: {} parallelism: {}}}",
          name,
          peers,
          request_size,
          duration.count(),
          max_duration.count(),
          parallelism);
    }
};

struct cloudcheck_opts
  : serde::
      envelope<cloudcheck_opts, serde::version<0>, serde::compat_version<0>> {
    // Descriptive name given to test run
    ss::sstring name{"Cloud credentials check"};

    // Timeout duration for cloud storage requests.
    ss::lowres_clock::duration timeout{std::chrono::milliseconds(10000)};

    // Backoff duration for cloud storage requests.
    ss::lowres_clock::duration backoff{std::chrono::milliseconds(10)};

    // Scheduling group that the benchmark will operate under.
    ss::scheduling_group sg;

    static cloudcheck_opts from_json(const json::Value& obj) {
        // The application using these parameters will perform any validation
        cloudcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("timeout_ms")) {
            opts.timeout = std::chrono::milliseconds(
              obj["timeout_ms"].GetInt());
        }
        if (obj.HasMember("backoff_ms")) {
            opts.backoff = std::chrono::milliseconds(
              obj["backoff_ms"].GetInt());
        }
        return opts;
    }

    auto serde_fields() { return std::tie(name, timeout, backoff); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{name: {} timeout: {} backoff: {}}}",
          name,
          timeout,
          backoff);
    }
};

// Captures unparsed test types passed to self test backend.
struct unparsed_check
  : serde::
      envelope<unparsed_check, serde::version<0>, serde::compat_version<0>> {
    ss::sstring test_type;
    ss::sstring test_json;
    auto serde_fields() { return std::tie(test_type, test_json); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{test_type: {}, test_json: {}}}",
          test_type,
          test_json);
    }
};

struct self_test_result
  : serde::
      envelope<self_test_result, serde::version<1>, serde::compat_version<0>> {
    double p50{0};
    double p90{0};
    double p99{0};
    double p999{0};
    double max{0};
    uint64_t rps{0};
    uint64_t bps{0};
    uint32_t timeouts{0};
    uuid_t test_id;
    ss::sstring name;
    ss::sstring info;
    ss::sstring test_type;
    uint64_t
      start_time{}; // lowres_clock::time_point is not serializable in serde
    uint64_t end_time{};
    ss::lowres_clock::duration duration{};
    std::optional<ss::sstring> warning;
    std::optional<ss::sstring> error;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{p50: {} p90: {} p99: {} p999: {} max: {} rps: {} bps: {} "
          "timeouts: {} test_id: {} name: {} info: {} type: {} start_time: {} "
          "end_time: {} duration: {}ms "
          "warning: {} error: {}}}",
          p50,
          p90,
          p99,
          p999,
          max,
          rps,
          bps,
          timeouts,
          test_id,
          name,
          info,
          test_type,
          start_time,
          end_time,
          std::chrono::duration_cast<std::chrono::milliseconds>(duration)
            .count(),
          warning ? *warning : "<no_value>",
          error ? *error : "<no_value>");
    }

    auto serde_fields() {
        return std::tie(
          p50,
          p90,
          p99,
          p999,
          max,
          rps,
          bps,
          timeouts,
          test_id,
          name,
          info,
          test_type,
          duration,
          warning,
          error,
          start_time,
          end_time);
    }
};

struct empty_request
  : serde::
      envelope<empty_request, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(); }
};

struct start_test_request
  : serde::envelope<
      start_test_request,
      serde::version<2>,
      serde::compat_version<0>> {
    uuid_t id;
    std::vector<diskcheck_opts> dtos;
    std::vector<netcheck_opts> ntos;
    std::vector<unparsed_check> unparsed_checks;
    std::vector<cloudcheck_opts> ctos;

    auto serde_fields() {
        return std::tie(id, dtos, ntos, unparsed_checks, ctos);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "{{id: {} ", id);
        for (const auto& v : dtos) {
            it = fmt::format_to(it, "diskcheck_opts: {}", v);
        }
        for (const auto& v : ntos) {
            it = fmt::format_to(it, "netcheck_opts: {}", v);
        }
        for (const auto& v : ctos) {
            it = fmt::format_to(it, "cloudcheck_opts: {}", v);
        }
        for (const auto& v : unparsed_checks) {
            it = fmt::format_to(it, "unparsed_check: {}", v);
        }
        return fmt::format_to(it, "}}");
    }
};

struct get_status_response
  : serde::envelope<
      get_status_response,
      serde::version<1>,
      serde::compat_version<0>> {
    uuid_t id{};
    self_test_status status{};
    std::vector<self_test_result> results;
    self_test_stage stage{};

    auto serde_fields() { return std::tie(id, status, results, stage); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{id: {} status: {} stage: {} test_results: {}}}",
          id,
          status,
          stage,
          results);
    }
};

struct netcheck_request
  : serde::
      envelope<netcheck_request, serde::version<0>, serde::compat_version<0>> {
    model::node_id source;
    iobuf buf;
    auto serde_fields() { return std::tie(source, buf); }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "{{source: {} buf: {}}}", source, buf.size_bytes());
    }
};

struct netcheck_response
  : serde::
      envelope<netcheck_response, serde::version<0>, serde::compat_version<0>> {
    size_t bytes_read{0};

    auto serde_fields() { return std::tie(bytes_read); }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{bytes_read: {}}}", bytes_read);
    }
};

/// Creates a netcheck_request with the buffer initialized to random data. The
/// buffer will be split into fragments of 8192 bytes each.
ss::future<cluster::netcheck_request>
make_netcheck_request(model::node_id src, size_t sz);

// Parses the raw json out of the start_test_request::unparsed_checks vector
// into self-test options for the various tests, utilizing `opt_t::from_json`.
// In the case that the controller node is of a redpanda version lower than
// the current node, some self-test checks may have been left in
// the "unparsed_checks" vector in the request when the server first processes
// the self test request. We will attempt to parse the test json in the
// self_test_backend of the follower node instead, if we recognize it.
void parse_self_test_checks(start_test_request& r);

} // namespace cluster
