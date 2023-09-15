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
#include "cluster/partition_manager.h"
#include "cluster/plugin_frontend.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "raft/group_manager.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "transform/io.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "transform/transform_manager.h"
#include "transform/transform_processor.h"
#include "units.h"
#include "utils/mutex.h"
#include "utils/uuid.h"
#include "wasm/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <exception>
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
  const model::transform_name& name,
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

class plugin_registry_adapter : public registry {
public:
    plugin_registry_adapter(
      cluster::plugin_frontend* pf, cluster::partition_manager* m)
      : _pf(pf)
      , _manager(m) {}

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

    absl::flat_hash_set<model::transform_id>
    lookup_by_input_topic(model::topic_namespace_view tp_ns) const override {
        auto entries = _pf->lookup_transforms_by_input_topic(tp_ns);
        absl::flat_hash_set<model::transform_id> result;
        for (const auto& [id, _] : entries) {
            result.emplace(id);
        }
        return result;
    }

    std::optional<model::transform_metadata>
    lookup_by_id(model::transform_id id) const override {
        return _pf->lookup_transform(id);
    }

private:
    cluster::plugin_frontend* _pf;
    cluster::partition_manager* _manager;
};

using runtime_proxy = ss::noncopyable_function<ss::future<
  ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>(model::transform_metadata)>;

class proc_factory : public processor_factory {
public:
    proc_factory(
      runtime_proxy runtime,
      std::unique_ptr<source::factory> source_factory,
      std::unique_ptr<sink::factory> sink_factory)
      : _runtime(std::move(runtime))
      , _source_factory(std::move(source_factory))
      , _sink_factory(std::move(sink_factory)) {}

    ss::future<std::unique_ptr<processor>> create_processor(
      model::transform_id id,
      model::ntp ntp,
      model::transform_metadata meta,
      processor::error_callback error_cb,
      probe* p) final {
        auto u = co_await _mu.get_units();
        auto& engine = _cache[meta.source_ptr];
        if (!engine) {
            auto factory = co_await _runtime(meta);
            if (!factory) {
                throw std::runtime_error("unable to fetch binary");
            }
            engine = co_await factory->make_engine();
        }
        auto src = *_source_factory->create(ntp);
        const auto& output_topic = meta.output_topics[0];
        std::vector<std::unique_ptr<sink>> sinks;
        sinks.push_back(*_sink_factory->create(
          model::ntp(output_topic.ns, output_topic.tp, ntp.tp.partition)));
        co_return std::make_unique<processor>(
          id,
          ntp,
          meta,
          std::move(engine),
          std::move(error_cb),
          std::move(src),
          std::move(sinks),
          p);
    }

private:
    mutex _mu;
    runtime_proxy _runtime;
    std::unique_ptr<source::factory> _source_factory;
    std::unique_ptr<sink::factory> _sink_factory;
    absl::flat_hash_map<model::offset, std::unique_ptr<wasm::engine>> _cache;
};

class rpc_client_sink final : public sink {
public:
    rpc_client_sink(model::ntp ntp, rpc::client* c)
      : _ntp(std::move(ntp))
      , _client(c) {}

    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override {
        auto resp = co_await _client->produce(_ntp.tp, std::move(batches));
        if (resp != cluster::errc::success) {
            // TODO: A better exception type or return a status code.
            throw std::runtime_error(
              ss::format("failure to produce transform data: {}", resp));
        }
    }

private:
    model::ntp _ntp;
    rpc::client* _client;
};

class rpc_client_factory final : public sink::factory {
public:
    explicit rpc_client_factory(rpc::client* c)
      : _client(c) {}

    std::optional<std::unique_ptr<sink>> create(model::ntp ntp) override {
        return std::make_unique<rpc_client_sink>(ntp, _client);
    };

private:
    rpc::client* _client;
};

class partition_source_factory;

class partition_source final : public source {
public:
    using factory = partition_source_factory;
    explicit partition_source(kafka::partition_proxy p)
      : _partition(std::move(p)) {}

    cluster::notification_id_type
    register_on_write_notification(ss::noncopyable_function<void()> cb) final {
        return _partition.register_on_write_notification(std::move(cb));
    }

    void
    unregister_on_write_notification(cluster::notification_id_type id) final {
        _partition.unregister_on_write_notification(id);
    }

    ss::future<model::offset> load_latest_offset() final {
        // Use read_committed latest offset
        auto result = _partition.last_stable_offset();
        if (result.has_error()) {
            throw kafka::exception(
              result.error(), kafka::make_error_code(result.error()).message());
        }
        co_return result.value();
    }

    ss::future<model::record_batch_reader>
    read_batch(model::offset offset, ss::abort_source* as) final {
        auto translater = co_await _partition.make_reader(
          storage::log_reader_config(
            /*start_offset=*/offset,
            /*max_offset=*/model::offset::max(),
            // TODO: Make a new priority for WASM transforms
            /*prio=*/kafka_read_priority(),
            /*as=*/*as));
        co_return std::move(translater).reader;
    }

private:
    kafka::partition_proxy _partition;
};

class partition_source_factory final : public source::factory {
public:
    explicit partition_source_factory(cluster::partition_manager* manager)
      : _manager(manager) {}

    std::optional<std::unique_ptr<source>> create(model::ntp ntp) final {
        auto p = _manager->get(ntp);
        if (!p) {
            return std::nullopt;
        }
        return std::make_unique<partition_source>(kafka::partition_proxy(
          std::make_unique<kafka::replicated_partition>(p)));
    };

private:
    cluster::partition_manager* _manager;
};

constexpr size_t max_wasm_binary_size = 5_MiB;
} // namespace

service::service(
  wasm::runtime* rt,
  model::node_id self,
  ss::sharded<cluster::plugin_frontend>* plugins,
  ss::sharded<features::feature_table>* features,
  ss::sharded<raft::group_manager>* leaders,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<rpc::client>* transform_client,
  const kafka::client::configuration& client_config)
  : _runtime(rt)
  , _self(self)
  , _plugins(plugins)
  , _leaders(leaders)
  , _features(features)
  , _partition_manager(partition_manager)
  , _transform_client(transform_client)
  , _client(std::make_unique<kafka::client::client>(
      config::to_yaml(client_config, config::redact_secrets::no)))
  , _manager(nullptr) {}

service::~service() = default;

ss::future<> service::start() {
    std::unique_ptr<partition_source_factory> source_factory
      = std::make_unique<partition_source_factory>(
        &_partition_manager->local());
    std::unique_ptr<sink::factory> sink_factory
      = std::make_unique<rpc_client_factory>(&_transform_client->local());

    _manager = std::make_unique<manager<ss::lowres_clock>>(
      std::make_unique<plugin_registry_adapter>(
        &_plugins->local(), &_partition_manager->local()),
      std::make_unique<proc_factory>(
        [this](model::transform_metadata meta) {
            return container().invoke_on(
              0, [meta = std::move(meta)](service& s) mutable {
                  return s.make_factory(std::move(meta));
              });
        },
        std::move(source_factory),
        std::move(sink_factory)));

    try {
        co_await _client->connect();
    } catch (...) {
        auto ex = std::current_exception();
        vlog(
          tlog.error,
          "unable to connect transforms kafka client to {}: {}",
          _client->config().brokers.value(),
          ex);
        std::rethrow_exception(ex);
    }
    co_await _manager->start();
    register_notifications();
}
void service::register_notifications() {
    _plugin_notification_id = _plugins->local().register_for_updates(
      [this](model::transform_id id) { _manager->on_plugin_change(id); });
    _leader_notification_id
      = _leaders->local().register_leadership_notification(
        [this](
          raft::group_id group_id,
          model::term_id,
          std::optional<model::node_id> leader) {
            auto partition = _partition_manager->local().partition_for(
              group_id);
            if (!partition) {
                vlog(
                  tlog.debug,
                  "got leadership notification for unknown partition: {}",
                  group_id);
                return;
            }
            bool node_is_leader = leader.has_value() && leader == _self;
            if (!node_is_leader) {
                _manager->on_leadership_change(
                  partition->ntp(), ntp_leader::no);
                return;
            }
            if (partition->ntp().ns != model::kafka_namespace) {
                return;
            }
            ntp_leader is_leader = partition && partition->is_elected_leader()
                                     ? ntp_leader::yes
                                     : ntp_leader::no;
            _manager->on_leadership_change(partition->ntp(), is_leader);
        });
    _partition_unmanage_notification_id
      = _partition_manager->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            _manager->on_leadership_change(
              model::ntp(model::kafka_namespace, model::topic_partition(tp)),
              ntp_leader::no);
        });
    // NOTE: this will also trigger notifications for existing partitions, which
    // will effectively bootstrap the transform manager.
    _partition_manage_notification_id
      = _partition_manager->local().register_manage_notification(
        model::kafka_namespace,
        [this](const ss::lw_shared_ptr<cluster::partition>& p) {
            ntp_leader is_leader = p->is_elected_leader() ? ntp_leader::yes
                                                          : ntp_leader::no;
            _manager->on_leadership_change(p->ntp(), is_leader);
        });
}

ss::future<> service::stop() {
    // Shutdown external requests
    unregister_notifications();
    co_await _gate.close();
    // Shutdown internal processes
    if (_manager) {
        co_await _manager->stop();
    }
    co_await _client->stop();
}
void service::unregister_notifications() {
    _plugins->local().unregister_for_updates(_plugin_notification_id);
    _leaders->local().unregister_leadership_notification(
      _leader_notification_id);
    _partition_manager->local().unregister_manage_notification(
      _partition_manage_notification_id);
    _partition_manager->local().unregister_unmanage_notification(
      _partition_unmanage_notification_id);
}

ss::future<cluster::errc>
service::deploy_transform(model::transform_metadata meta, iobuf buf) {
    if (!_features->local().is_active(features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();
    auto name = meta.name;
    bool is_valid = co_await validate_source(
      meta, buf.share(0, buf.size_bytes()));
    if (!is_valid) {
        co_return cluster::errc::transform_invalid_source;
    }
    auto [key, offset] = co_await write_source(name, std::move(buf));
    vlog(tlog.debug, "wrote wasm source at key={} offset={}", key, offset);
    meta.uuid = key;
    meta.source_ptr = offset;
    auto errc = co_await _plugins->local().upsert_transform(
      std::move(meta), model::no_timeout);
    if (errc != cluster::errc::success) {
        // TODO: This is a best effort cleanup, we should also have some sort of
        // GC process (using delete_records) as well.
        co_await write_source_tombstone(key, name);
    }
    co_return errc;
}

ss::future<cluster::errc>
service::delete_transform(model::transform_name name) {
    if (!_features->local().is_active(features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();
    auto result = co_await _plugins->local().remove_transform(
      name, model::no_timeout);
    // Make deletes itempotent by translating does not exist into success
    if (result.ec == cluster::errc::transform_does_not_exist) {
        co_return cluster::errc::success;
    }
    if (result.ec != cluster::errc::success) {
        co_return result.ec;
    }
    // We still want to tombstone a record in case of a transform's metadata was
    // removed, but this write failed in another request.
    co_await write_source_tombstone(result.uuid, name);
    co_return cluster::errc::success;
}

std::vector<model::transform_metadata> service::list_transforms() {
    auto transforms = _plugins->local().all_transforms();
    std::vector<model::transform_metadata> output;
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
      .name = model::wasm_plugin_internal_tp.topic,
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
        topic_config(kafka::topic_property_max_message_bytes, ss::format("{}", max_wasm_binary_size)),
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
service::write_source(model::transform_name name, iobuf buf) {
    // TODO: Do this lazily
    co_await create_internal_source_topic();
    auto key = uuid_t::create();
    model::record_batch batch = make_batch(
      key, name, std::move(buf), tombstone::no);
    auto resp = co_await _client->produce_record_batch(
      model::wasm_plugin_internal_tp, std::move(batch));
    check_error_code(resp);
    co_return std::make_pair(key, resp.base_offset);
}
ss::future<>
service::write_source_tombstone(uuid_t key, model::transform_name name) {
    // TODO: Do this lazily
    co_await create_internal_source_topic();
    model::record_batch batch = make_batch(
      key, name, std::nullopt, tombstone::yes);
    auto resp = co_await _client->produce_record_batch(
      model::wasm_plugin_internal_tp, std::move(batch));
    check_error_code(resp);
}
ss::future<bool>
service::validate_source(model::transform_metadata meta, iobuf buf) {
    // TODO: This size isn't exactly correct as it doesn't account for the
    // "serialized" as a batch size
    if (buf.size_bytes() > max_wasm_binary_size) {
        co_return false;
    }
    // Validate that the source is good by just creating a transform, but don't
    // run anything we should probably expose a better API in runtime for
    // this, even if it just does this...
    auto factory = co_await _runtime->make_factory(
      std::move(meta), std::move(buf), &tlog);
    auto engine = co_await factory->make_engine();
    co_await engine->start();
    bool is_valid = true;
    try {
        co_await engine->initialize();
    } catch (const std::exception& ex) {
        vlog(
          tlog.info,
          "transform {} failed to be initialized: {}",
          meta.name,
          ex);
        is_valid = false;
    }
    co_await engine->stop();
    co_return is_valid;
}

ss::future<std::optional<iobuf>>
service::fetch_binary(model::offset offset, std::chrono::milliseconds timeout) {
    // Only request a single byte, which essentially ensures we only fetch a
    // single record (as KIP-74 says we'll always return a single record to
    // make progress).
    constexpr int max_fetch_bytes = 1;
    auto resp = co_await _client->fetch_partition(
      model::wasm_plugin_internal_tp, offset, max_fetch_bytes, timeout);
    if (resp.data.error_code == kafka::error_code::offset_out_of_range) {
        co_return std::nullopt;
    }
    check_error_code(resp.data);
    vassert(resp.data.topics.size() == 1, "unexpected topics size");
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
    for (const auto& header : record.headers()) {
        if (header.key() == state_header_key) {
            if (header.value() == string_to_iobuf("tombstone")) {
                co_return std::nullopt;
            }
        }
    }
    co_return record.release_value();
}

namespace {
class proxy_factory final : public wasm::factory {
public:
    explicit proxy_factory(std::unique_ptr<wasm::factory> factory)
      : _factory(std::move(factory)) {}

    ss::future<std::unique_ptr<wasm::engine>> make_engine() final {
        return _factory->make_engine();
    }

private:
    std::unique_ptr<wasm::factory> _factory;
};
} // namespace

ss::future<ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>
service::make_factory(model::transform_metadata meta) {
    auto units = co_await _mu.get_units();
    auto& factory = _factory_cache[meta.source_ptr];
    if (factory) {
        co_return ss::make_foreign(factory);
    }
    vlog(tlog.info, "compiling wasm for {} at {}", meta.name, meta.source_ptr);
    constexpr auto timeout = 10s;
    auto binary = co_await fetch_binary(meta.source_ptr, timeout);
    if (!binary) {
        co_return nullptr;
    }
    auto shared = ss::make_shared<proxy_factory>(
      co_await _runtime->make_factory(meta, *std::move(binary), &tlog));
    factory = ss::static_pointer_cast<wasm::factory, proxy_factory>(shared);
    co_return ss::make_foreign(factory);
}

} // namespace transform
