// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/types.h"

#include "hashing/crc32c.h"
#include "reflection/for_each_field.h"

#include <seastar/core/byteorder.hh>

#include <boost/crc.hpp>
#include <fmt/format.h>

namespace rpc {
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
void crc_one(crc::crc32c& crc, T t) {
    T args_le = ss::cpu_to_le(t);
    crc.extend(args_le);
}

uint32_t checksum_header_only(const header& h) {
    auto crc = crc::crc32c();
    crc_one(
      crc,
      static_cast<std::underlying_type_t<compression_type>>(h.compression));
    crc_one(crc, h.payload_size);
    crc_one(crc, h.meta);
    crc_one(crc, h.correlation_id);
    crc_one(crc, h.payload_checksum);
    return crc.value();
}

fmt::iterator header::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{version:{}, header_checksum:{}, compression:{}, "
      "payload_size:{}, meta:{}, correlation_id:{}, payload_checksum:{}}}",
      static_cast<int>(version),
      header_checksum,
      static_cast<int>(compression),
      payload_size,
      meta,
      correlation_id,
      payload_checksum);
}

std::string_view format_as(status s) {
    switch (s) {
    case status::success:
        return "rpc::status::success";
    case status::method_not_found:
        return "rpc::status::method_not_found";
    case status::request_timeout:
        return "rpc::status::request_timeout";
    case status::server_error:
        return "rpc::status::server_error";
    case status::version_not_supported:
        return "rpc::status::version_not_supported";
    case status::service_unavailable:
        return "rpc::status::service_unavailable";
    default:
        return "rpc::status::unknown";
    }
}

std::ostream& operator<<(std::ostream& o, const status& s) {
    return o << format_as(s);
}

std::string_view format_as(transport_version v) {
    switch (v) {
    case transport_version::v0:
        return "rpc::transport_version::v0";
    case transport_version::v1:
        return "rpc::transport_version::v1";
    case transport_version::v2:
        return "rpc::transport_version::v2";
    default:
        return "rpc::transport_version::unknown";
    }
}

std::ostream& operator<<(std::ostream& o, transport_version v) {
    return o << format_as(v);
}

} // namespace rpc
