/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/rest/kvstore_handlers.h"

#include "bytes/iostream.h"
#include "config/broker_authn_endpoint.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/serde.h"
#include "model/namespace.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/httpd.h"
#include "proto/redpanda/core/rest/v1/kvstore.proto.h"
#include "security/acl.h"
#include "security/authorizer.h"

#include <type_traits>

namespace pandaproxy::rest {

namespace {

void check_errc(cluster::errc ec) {
    using status = ss::http::reply::status_type;
    switch (ec) {
    case cluster::errc::success:
        return;
    case cluster::errc::generic_tx_error:
        throw ss::httpd::base_exception(
          "precondition failed", status::conflict);
    case cluster::errc::topic_not_exists:
        throw ss::httpd::base_exception("topic not found", status::not_found);
    case cluster::errc::not_leader:
        throw ss::httpd::base_exception(
          "not leader", status::service_unavailable);
    case cluster::errc::invalid_request:
        throw ss::httpd::bad_request_exception("invalid request");
    case cluster::errc::partition_operation_failed:
        throw ss::httpd::base_exception(
          "partition operation failed", status::service_unavailable);
    default:
        vlog(plog.error, "unknown status code: {}", ec);
        throw ss::httpd::base_exception(
          "unknown server error", status::internal_server_error);
    }
}

constexpr size_t max_key_size = 16_KiB;

ss::sstring linearize_key(const iobuf& k) {
    if (k.size_bytes() > max_key_size) {
        throw ss::httpd::bad_request_exception(
          fmt::format("key too large {} > {}", k.size_bytes(), max_key_size));
    }
    return k.linearize_to_string();
}

// Helper to convert proto precondition to RPC precondition
kafka::data::rpc::kv_precondition convert_precondition(
  const proto::pandaproxy::kv_store_write_request_precondition& proto_precond) {
    if (!proto_precond.has_kind()) {
        return kafka::data::rpc::kv_no_precondition{};
    }
    if (proto_precond.has_if_exists()) {
        return kafka::data::rpc::kv_precondition_if_exists{
          proto_precond.get_if_exists().get_exists()};
    }
    if (proto_precond.has_if_matches()) {
        return kafka::data::rpc::kv_precondition_if_matches{
          proto_precond.get_if_matches().get_sha256_hash()};
    }
    return kafka::data::rpc::kv_no_precondition{};
}

kafka::data::rpc::kv_write_request
convert(proto::pandaproxy::kv_store_write_request proto_req) {
    kafka::data::rpc::kv_write_request rpc_req;
    // Convert puts
    for (auto& proto_put : proto_req.get_puts()) {
        kafka::data::rpc::kv_entry entry{
          linearize_key(proto_put.get_key()), proto_put.get_value().copy()};
        kafka::data::rpc::kv_put rpc_put{
          std::move(entry), convert_precondition(proto_put.get_precondition())};
        rpc_req.puts.push_back(std::move(rpc_put));
    }
    // Convert deletes
    for (auto& proto_del : proto_req.get_deletes()) {
        kafka::data::rpc::kv_remove rpc_remove{
          linearize_key(proto_del.get_key()),
          convert_precondition(proto_del.get_precondition())};
        rpc_req.removals.push_back(std::move(rpc_remove));
    }
    return rpc_req;
}

proto::pandaproxy::kv_store_write_response
convert(kafka::data::rpc::kv_write_reply) {
    return proto::pandaproxy::kv_store_write_response{};
}

kafka::data::rpc::kv_get_request
convert(proto::pandaproxy::kv_store_get_request proto_req) {
    kafka::data::rpc::kv_get_request rpc_req;
    for (auto& key_buf : proto_req.get_keys()) {
        rpc_req.keys.push_back(linearize_key(key_buf));
    }
    return rpc_req;
}

proto::pandaproxy::kv_store_get_response
convert(kafka::data::rpc::kv_get_reply rpc_reply) {
    proto::pandaproxy::kv_store_get_response proto_resp;
    for (auto& rpc_result : rpc_reply.results) {
        proto::pandaproxy::kv_store_get_response_lookup_result lookup;
        lookup.set_key(iobuf::from(rpc_result.key));
        if (rpc_result.value.has_value()) {
            lookup.set_value(std::move(rpc_result.value.value()));
        }
        proto_resp.get_results().push_back(std::move(lookup));
    }
    return proto_resp;
}

kafka::data::rpc::kv_scan_request
convert(proto::pandaproxy::kv_store_scan_request proto_req) {
    kafka::data::rpc::kv_scan_request rpc_req;
    if (proto_req.has_start_key()) {
        rpc_req.start_key = linearize_key(proto_req.get_start_key());
    }
    if (proto_req.has_end_key()) {
        rpc_req.end_key = linearize_key(proto_req.get_end_key());
    }
    constexpr int32_t max_limit = 1000;
    rpc_req.limit = static_cast<uint32_t>(
      std::clamp(proto_req.get_limit(), 0, max_limit));
    if (rpc_req.limit == 0) {
        constexpr uint32_t default_limit = 500;
        rpc_req.limit = default_limit;
    }
    return rpc_req;
}

proto::pandaproxy::kv_store_scan_response
convert(kafka::data::rpc::kv_scan_reply rpc_reply) {
    proto::pandaproxy::kv_store_scan_response proto_resp;
    for (auto& rpc_entry : rpc_reply.entries) {
        proto::pandaproxy::kv_store_scan_response_entry entry;
        entry.set_key(iobuf::from(rpc_entry.key));
        entry.set_value(std::move(rpc_entry.value));
        proto_resp.get_entries().push_back(std::move(entry));
    }
    return proto_resp;
}

template<
  typename T,
  typename U = std::
    invoke_result_t<decltype([](T t) { return convert(std::move(t)); }), T>>
ss::future<U> parse_request(proxy::server::request_t* rq) {
    auto req_fmt = parse::content_type_header(
      *rq->req,
      {json::serialization_format::application_json,
       json::serialization_format::application_proto});
    auto data = co_await read_iobuf_exactly(
      *rq->req->content_stream, rq->context().max_memory);
    U converted;
    if (req_fmt == json::serialization_format::application_json) {
        converted = convert(co_await T::from_json(std::move(data)));
    } else {
        converted = convert(co_await T::from_proto(std::move(data)));
    }
    converted.ntp = {model::ntp{
      model::kafka_namespace,
      parse::request_param<model::topic>(*rq->req, "topic_name"),
      parse::request_param<model::partition_id>(*rq->req, "partition_id")}};
    co_return converted;
}

ss::future<> write_reply(
  proxy::server::request_t* rq, proxy::server::reply_t* rp, auto reply_body) {
    check_errc(reply_body.err);
    auto req_fmt = parse::content_type_header(
      *rq->req,
      {json::serialization_format::application_json,
       json::serialization_format::application_proto});
    auto res_fmt = parse::accept_header(
      *rq->req,
      {req_fmt,
       json::serialization_format::application_json,
       json::serialization_format::application_proto});
    iobuf reply_payload;
    if (res_fmt == json::serialization_format::application_json) {
        reply_payload = co_await convert(std::move(reply_body)).to_json();
    } else {
        reply_payload = co_await convert(std::move(reply_body)).to_proto();
    }
    rp->rep->set_status(ss::http::reply::status_type::ok);
    rp->mime_type = res_fmt;
    rp->rep->write_body(
      "bin",
      [payload = std::move(reply_payload)](
        ss::output_stream<char>& writer) mutable {
          return write_iobuf_to_output_stream(payload.share(), writer);
      });
}

[[nodiscard]]
security::auth_result check_authz(
  proxy::server::request_t& rq,
  const model::topic& topic,
  security::acl_operation op) {
    security::auth_result authz_result;
    if (config::kafka_authz_enabled()) {
        authz_result = rq.context().authorizer->authorized(
          topic,
          op,
          security::acl_principal{security::principal_type::user, rq.user.name},
          security::acl_host{rq.req->get_client_address().addr()},
          security::superuser_required::no,
          {});
    } else {
        authz_result = security::auth_result::authz_disabled(
          security::acl_principal{security::principal_type::user, rq.user.name},
          security::acl_host{rq.req->get_client_address().addr()},
          op,
          topic);
    }
    // TODO: Audit! We also need to audit failures during authn
    return authz_result;
}

} // namespace

ss::future<proxy::server::reply_t>
kv_write(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto req
      = co_await parse_request<proto::pandaproxy::kv_store_write_request>(&rq);
    auto result = check_authz(
      rq, req.ntp.tp.topic, security::acl_operation::write);
    kafka::data::rpc::client* rpc_client = rq.context().rpc_client;
    auto reply = co_await rpc_client->kv_write(std::move(req));
    co_await write_reply(&rq, &rp, reply);
    co_return rp;
}

ss::future<proxy::server::reply_t>
kv_get(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto req = co_await parse_request<proto::pandaproxy::kv_store_get_request>(
      &rq);
    auto result = check_authz(
      rq, req.ntp.tp.topic, security::acl_operation::read);
    kafka::data::rpc::client* rpc_client = rq.context().rpc_client;
    auto reply = co_await rpc_client->kv_get(std::move(req));
    co_await write_reply(&rq, &rp, std::move(reply));
    co_return rp;
}

ss::future<proxy::server::reply_t>
kv_scan(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto req = co_await parse_request<proto::pandaproxy::kv_store_scan_request>(
      &rq);
    auto result = check_authz(
      rq, req.ntp.tp.topic, security::acl_operation::read);
    kafka::data::rpc::client* rpc_client = rq.context().rpc_client;
    auto reply = co_await rpc_client->kv_scan(std::move(req));
    co_await write_reply(&rq, &rp, std::move(reply));
    co_return rp;
}

} // namespace pandaproxy::rest
