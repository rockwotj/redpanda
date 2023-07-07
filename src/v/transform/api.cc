/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "transform/api.h"

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/plugin_frontend.h"
#include "cluster/types.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "transform/io.h"
#include "transform/logger.h"
#include "transform/transform_manager.h"
#include "utils/uuid.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>

#include <__concepts/same_as.h>

#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace transform {

namespace {

template<typename T>
concept KafkaResponseWithMessage = requires(T t) {
    { t.error_code } -> std::same_as<kafka::error_code>;
    { t.error_message } -> std::same_as<std::optional<ss::sstring>>;
};

template<typename T>
void check_error_code(const T& resp) {
    if (resp.error_code != kafka::error_code::none) {
        if constexpr (KafkaResponseWithMessage<T>) {
            throw kafka::exception(
              resp.error_code,
              resp.error_message.value_or(
                kafka::make_error_code(resp.error_code).message()));
        } else {
            throw kafka::exception(
              resp.error_code,
              kafka::make_error_code(resp.error_code).message());
        }
    }
}

kafka::createable_topic_config
topic_config(std::string_view name, std::string_view value) {
    return {.name = ss::sstring(name), .value = ss::sstring(value)};
}

iobuf string_to_iobuf(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

using tombstone = ss::bool_class<struct tombstone_tag>;
model::record_batch make_batch(
  const uuid_t& key,
  const cluster::transform_name& name,
  std::optional<iobuf> value,
  tombstone is_tombstone) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    iobuf k;
    k.append(key.uuid().begin(), key.uuid().size());
    std::vector<model::record_header> headers;
    headers.emplace_back(string_to_iobuf("name"), string_to_iobuf(name()));
    headers.emplace_back(
      string_to_iobuf("state"),
      is_tombstone == tombstone::yes ? string_to_iobuf("tombstone")
                                     : string_to_iobuf("live"));
    builder.add_raw_kw(std::move(k), std::move(value), std::move(headers));
    return std::move(builder).build();
}

class plugin_registry_adapter : public plugin_registry {
public:
    plugin_registry_adapter(
      cluster::plugin_frontend* pf,
      kafka::client::client* c,
      cluster::partition_manager* m,
      ss::gate* g)
      : _pf(pf)
      , _client(c)
      , _manager(m)
      , _gate(g) {}

    absl::flat_hash_set<model::partition_id>
    get_leader_partitions(model::topic_namespace_view tp_ns) const override {
        absl::flat_hash_set<model::partition_id> p;
        for (const auto& entry : _manager->get_topic_partition_table(tp_ns)) {
            if (entry.second->is_elected_leader()) {
                p.emplace(entry.first.tp.partition);
            }
        }
        return p;
    }

    absl::flat_hash_map<cluster::transform_id, cluster::transform_metadata>
    lookup_by_input_topic(model::topic_namespace_view tp_ns) const override {
        return _pf->lookup_transforms_by_input_topic(tp_ns);
    }

    std::optional<cluster::transform_metadata>
    lookup_by_id(cluster::transform_id id) const override {
        return _pf->lookup_transform(id);
    }

    void report_error(
      cluster::transform_id, cluster::transform_metadata meta) const override {
        ssx::spawn_with_gate(*_gate, [this, meta = std::move(meta)]() mutable {
            constexpr auto timeout = 300ms;
            meta.paused = true;
            return _pf
              ->upsert_transform(
                std::move(meta), model::timeout_clock::now() + timeout)
              .discard_result();
        });
    }

    ss::future<std::optional<iobuf>> fetch_binary(
      model::offset offset, std::chrono::milliseconds timeout) const override {
        // Only request a single byte, which essentailly ensures we only fetch a
        // single record (as KIP-74 says we'll always return a single record to
        // make progress).
        constexpr int max_fetch_bytes = 1;
        auto resp = co_await _client->fetch_partition(
          model::wasm_plugin_internal_tp, offset, max_fetch_bytes, timeout);
        if (resp.data.error_code == kafka::error_code::offset_out_of_range) {
            co_return std::nullopt;
        }
        check_error_code(resp.data);
        vassert(resp.data.topics.size() == 1, "unexpected topcs size");
        auto& t = resp.data.topics[0];
        vassert(t.partitions.size() == 1, "unexpected partitions size");
        auto& p = t.partitions[0];
        check_error_code(p);
        if (!p.records) {
            co_return std::nullopt;
        }
        auto reader = model::make_record_batch_reader<kafka::batch_reader>(
          std::move(*p.records));
        auto batches = co_await model::consume_reader_to_memory(
          std::move(reader), model::no_timeout);
        if (batches.empty()) {
            co_return std::nullopt;
        }
        vassert(batches.size() == 1, "unexpected batches size");
        vassert(batches[0].record_count() == 1, "unexpected batch size");
        auto records = batches[0].copy_records();
        auto& record = records.front();
        auto state_header_key = string_to_iobuf("state");
        tombstone state;
        for (const auto& header : record.headers()) {
            if (header.key() == state_header_key) {
                state = header.value() == string_to_iobuf("live")
                          ? tombstone::no
                          : tombstone::yes;
                break;
            }
        }
        if (state == tombstone::yes) {
            co_return std::nullopt;
        }
        co_return record.release_value();
    }

private:
    cluster::plugin_frontend* _pf;
    kafka::client::client* _client;
    cluster::partition_manager* _manager;
    ss::gate* _gate;
};

template<typename I, typename T>
class kafka_client_factory : public factory<I> {
public:
    explicit kafka_client_factory(kafka::client::client* c)
      : _client(c) {}

    std::unique_ptr<I> create(model::ntp ntp) override {
        return std::make_unique<T>(ntp, _client);
    };

private:
    kafka::client::client* _client;
};

class kafka_client_sink : public sink {
public:
    using factory = kafka_client_factory<sink, kafka_client_sink>;

    kafka_client_sink(model::ntp ntp, kafka::client::client* c)
      : _ntp(std::move(ntp))
      , _client(c) {}

    ss::future<> write(model::record_batch batch) override {
        auto resp = co_await _client->produce_record_batch(
          _ntp.tp, std::move(batch));
        check_error_code(resp);
    }

private:
    model::ntp _ntp;
    kafka::client::client* _client;
};

class kafka_client_source : public source {
public:
    using factory = kafka_client_factory<source, kafka_client_source>;

    kafka_client_source(model::ntp ntp, kafka::client::client* c)
      : _ntp(std::move(ntp))
      , _client(c) {}

    ss::future<model::offset> load_latest_offset() override {
        auto resp = co_await _client->list_offsets(_ntp.tp);
        if (resp.data.topics.size() != 1) {
            throw std::runtime_error("unexpected topic count");
        }
        auto partitions = resp.data.topics[0].partitions;
        if (partitions.size() != 1) {
            throw std::runtime_error("unexpected partition count");
        }
        auto p = partitions[0];
        check_error_code(p);
        co_return p.offset;
    }
    ss::future<std::optional<model::record_batch_reader>>
    read_batch(model::offset offset) override {
        auto resp = co_await _client->fetch_partition(_ntp.tp, offset, 1, 1ms);
        check_error_code(resp.data);
        if (resp.data.topics.size() != 1) {
            throw std::runtime_error("unexpected topic count");
        }
        auto& partitions = resp.data.topics.front().partitions;
        if (partitions.size() != 1) {
            throw std::runtime_error("unexpected partition count");
        }
        auto& p = partitions[0];
        check_error_code(p);
        if (!p.records) {
            co_return std::nullopt;
        }
        co_return model::record_batch_reader(
          std::make_unique<kafka::batch_reader>(std::move(*p.records)));
    }

private:
    model::ntp _ntp;
    kafka::client::client* _client;
};

} // namespace

service::service(
  wasm::runtime* rt,
  model::node_id self,
  cluster::plugin_frontend* plugins,
  cluster::partition_leaders_table* leaders,
  cluster::partition_manager* partition_manager,
  const kafka::client::configuration&)
  : _runtime(rt)
  , _self(self)
  , _plugins(plugins)
  , _leaders(leaders)
  , _partition_manager(partition_manager)
  , _client(std::make_unique<kafka::client::client>(config::to_yaml(
      kafka::client::configuration(), config::redact_secrets::no)))
  , _manager(nullptr) {
    std::unique_ptr<source::factory> source_factory
      = std::make_unique<kafka_client_source::factory>(_client.get());
    std::unique_ptr<sink::factory> sink_factory
      = std::make_unique<kafka_client_sink::factory>(_client.get());
    _manager = std::make_unique<manager>(
      _runtime,
      std::make_unique<plugin_registry_adapter>(
        plugins, _client.get(), partition_manager, &_gate),
      std::move(source_factory),
      std::move(sink_factory));
}

service::~service() = default;

ss::future<> service::start() {
    co_await _client->connect();
    co_await create_internal_source_topic();
    co_await _manager->start();
    register_notifications();
}
void service::register_notifications() {
    _plugin_notification_id = _plugins->register_for_updates(
      [this](cluster::transform_id id) { _manager->on_plugin_change(id); });
    _leader_notification_id = _leaders->register_leadership_change_notification(
      [this](
        model::ntp ntp, model::term_id, std::optional<model::node_id> leader) {
          if (ntp.ns != model::kafka_namespace) {
              return;
          }
          bool node_is_leader = leader.has_value() && leader == _self;
          if (!node_is_leader) {
              _manager->on_leadership_change(std::move(ntp), ntp_leader::no);
              return;
          }
          auto partition = _partition_manager->get(ntp);
          ntp_leader is_leader = partition && partition->is_elected_leader()
                                   ? ntp_leader::yes
                                   : ntp_leader::no;
          _manager->on_leadership_change(std::move(ntp), is_leader);
      });
    _partition_unmanage_notification_id
      = _partition_manager->register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition tp) {
            _manager->on_leadership_change(
              model::ntp(model::kafka_namespace, std::move(tp)),
              ntp_leader::no);
        });
    // NOTE: this will also trigger notifications for existing partitions, which
    // will effectively bootstrap the transform manager.
    _partition_manage_notification_id
      = _partition_manager->register_manage_notification(
        model::kafka_namespace,
        [this](const ss::lw_shared_ptr<cluster::partition>& p) {
            ntp_leader is_leader = p->is_leader() ? ntp_leader::yes
                                                  : ntp_leader::no;
            _manager->on_leadership_change(p->ntp(), is_leader);
        });
}

ss::future<> service::stop() {
    // Shutdown external requests
    unregister_notifications();
    co_await _gate.close();
    // Shutdown internal processes
    co_await _manager->stop();
    co_await _client->stop();
}
void service::unregister_notifications() {
    _plugins->unregister_for_updates(_plugin_notification_id);
    _leaders->unregister_leadership_change_notification(
      _leader_notification_id);
    _partition_manager->unregister_manage_notification(
      _partition_manage_notification_id);
    _partition_manager->unregister_unmanage_notification(
      _partition_unmanage_notification_id);
}

ss::future<cluster::errc>
service::deploy_transform(cluster::transform_metadata meta, iobuf buf) {
    auto _ = _gate.hold();
    auto name = meta.name;
    co_await validate_source(meta, buf.share(0, buf.size_bytes()));
    auto [key, offset] = co_await write_source(name, std::move(buf));
    meta.source_key = key;
    meta.source_ptr = offset;
    auto errc = co_await _plugins->upsert_transform(
      std::move(meta), model::no_timeout);
    if (errc != cluster::errc::success) {
        // TODO: This is a best effort cleanup, we should also have some sort of
        // GC process (using delete_records) as well.
        co_await write_source_tombstone(key, name);
    }
    co_return errc;
}

ss::future<cluster::errc>
service::delete_transform(cluster::transform_name name) {
    auto _ = _gate.hold();
    auto result = co_await _plugins->remove_transform(name, model::no_timeout);
    if (
      result.ec != cluster::errc::success
      && result.ec != cluster::errc::transform_does_not_exist) {
        co_return result.ec;
    }
    // We still want to tombstone a record in case of a transform's metadata was
    // removed, but this write failed in another request.
    co_await write_source_tombstone(result.source_key, name);
    // Make deletes itempotent by translating does not exist into success
    co_return cluster::errc::success;
}

std::vector<cluster::transform_metadata> service::list_transforms() {
    auto transforms = _plugins->all_transforms();
    std::vector<cluster::transform_metadata> output;
    output.reserve(transforms.size());
    for (auto& [_, v] : transforms) {
        output.push_back(std::move(v));
    }
    return output;
}

ss::future<> service::create_internal_source_topic() {
    auto _ = _gate.hold();
    constexpr std::string_view retain_forever = "-1";
    kafka::creatable_topic req{
      .name = model::wasm_plugin_internal_topic,
      .num_partitions = 1,
      // TODO: The health manager will fix this, but should probably just create it correctly according to config.
      .replication_factor = 1,
      .assignments = {},
      .configs = {
        topic_config(kafka::topic_property_cleanup_policy, "compact"),
        topic_config(kafka::topic_property_compression, "none"),
        topic_config(kafka::topic_property_retention_bytes, retain_forever),
        topic_config(kafka::topic_property_retention_duration, retain_forever),
        topic_config(kafka::topic_property_retention_local_target_bytes, retain_forever),
        topic_config(kafka::topic_property_retention_local_target_ms, retain_forever),
      },
    };
    auto resp = co_await _client->create_topic(req);
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error("unexpected topic count");
    }
    const auto& topic = resp.data.topics.front();
    if (topic.error_code == kafka::error_code::none) {
        vlog(tlog.debug, "wasm transforms: created internal topic");
    } else if (topic.error_code == kafka::error_code::topic_already_exists) {
        vlog(tlog.debug, "wasm transforms: found internal topic");
    } else if (topic.error_code == kafka::error_code::not_controller) {
        vlog(tlog.debug, "wasm transforms: not controller");
    } else {
        check_error_code(topic);
    }
}

ss::future<std::pair<uuid_t, model::offset>>
service::write_source(cluster::transform_name name, iobuf buf) {
    auto key = uuid_t::create();
    model::record_batch batch = make_batch(
      key, name, std::move(buf), tombstone::no);
    auto resp = co_await _client->produce_record_batch(
      model::wasm_plugin_internal_tp, std::move(batch));
    check_error_code(resp);
    co_return std::make_pair(key, resp.base_offset);
}
ss::future<>
service::write_source_tombstone(uuid_t key, cluster::transform_name name) {
    model::record_batch batch = make_batch(
      key, name, std::nullopt, tombstone::yes);
    auto resp = co_await _client->produce_record_batch(
      model::wasm_plugin_internal_tp, std::move(batch));
    check_error_code(resp);
}
ss::future<>
service::validate_source(cluster::transform_metadata meta, iobuf buf) {
    // Validate that the source is good by just creating a transform, but don't
    // run anything we should probably expose a better API in runtime for
    // this, even if it just does this...
    auto factory = co_await _runtime->make_factory(
      std::move(meta), std::move(buf));
    auto engine = co_await factory->make_engine();
    co_await engine->start();
    co_await engine->stop();
}

} // namespace transform
