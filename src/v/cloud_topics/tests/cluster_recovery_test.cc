/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/format_to.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_recovery_manager.h"
#include "cluster/cluster_recovery_table.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace cloud_topics;
using tests::kv_t;

namespace {

ss::logger test_log("cluster_recovery_test");

ss::abort_source never_abort;
const model::topic topic{"tapioca"};
const model::topic_namespace nt{model::kafka_namespace, topic};
const model::node_id n0{0};
constexpr int num_partitions = 10;

ss::future<> random_sleep_ms(int max_ms) {
    co_await ss::sleep(random_generators::get_int(max_ms) * 1ms);
}

struct test_params {
    bool unstable_controller{false};
    bool unstable_metastore{false};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "controller_{}_metastore_{}",
          unstable_controller ? "unstable" : "stable",
          unstable_metastore ? "unstable" : "stable");
    }
};

} // namespace

// Base class with all helper methods, not parameterized.
class CloudTopicsClusterRecoveryTestBase : public cluster_fixture {
public:
    cloud_topics::test_fixture_cfg fixture_cfg() const {
        return {
          .use_lsm_metastore = true,
          // Skip flushing since tests may exercise flushing.
          .skip_flush_loop = true,
        };
    }
    void SetUp() {
        cfg.get("enable_leader_balancer").set_value(false);
        cfg.get("enable_cluster_metadata_upload_loop").set_value(false);
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);

        for (size_t i = 0; i < 3; ++i) {
            add_node(fixture_cfg());
        }
        wait_for_all_members(5s).get();
    }

    using partition_ptr = ss::lw_shared_ptr<cluster::partition>;
    ss::future<>
    wait_for_leader(const model::ntp& ntp, partition_ptr* prt = nullptr) {
        SCOPED_TRACE(fmt::format("Waiting for leadership of ntp {}", ntp));
        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&] {
            auto [leader_fx, leader_p] = get_leader(ntp);
            if (leader_fx != nullptr) {
                if (prt) {
                    *prt = std::move(leader_p);
                }
                return true;
            }
            return false;
        });
    }

    ss::future<> leadership_change_fiber(bool& stop, model::ntp ntp) {
        while (!stop) {
            co_await random_sleep_ms(5000);
            try {
                partition_ptr leader_p;
                co_await wait_for_leader(ntp, &leader_p);
                co_await leader_p->raft()->step_down("test");
            } catch (...) {
                auto ex = std::current_exception();
                vlog(
                  test_log.warn,
                  "Exception in leader change fiber for {}: {}",
                  ntp,
                  ex);
            }
            co_await random_sleep_ms(5000);
        }
    }

    ss::future<> create_cloud_topic(model::topic t = topic) {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        co_await create_topic(
          {model::kafka_namespace, t}, num_partitions, 3, props);
    }

    ss::future<> produce_some() {
        for (int p = 0; p < num_partitions; ++p) {
            SCOPED_TRACE(fmt::format("Producing to partition {}", p));
            model::ntp ntp{
              model::kafka_namespace, topic, model::partition_id(p)};

            // Find the leader for this partition
            co_await wait_for_leader(ntp);
            auto [leader_fx, leader_p] = get_leader(ntp);
            RPTEST_REQUIRE_CORO(leader_fx != nullptr)
              << "No leader for partition " << p;
            auto leader_id = leader_p->raft()->self().id();

            auto* producer = co_await make_producer(leader_id);
            std::vector<kv_t> records;
            for (int i = 0; i < 100; ++i) {
                records.emplace_back(
                  ssx::sformat("key-{}-{}", p, i),
                  ssx::sformat("val-{}-{}", p, i));
            }
            co_await producer->produce_to_partition(
              topic, model::partition_id(p), kv_t::sequence(0, 100));
        }
    }

    model::topic_id get_topic_id(const model::topic_namespace& nt) {
        auto& topics_state
          = get_node_application(n0)->controller->get_topics_state().local();
        auto& topic_md = topics_state.topics_map().at(nt);
        auto topic_id = topic_md.get_configuration().tp_id;
        EXPECT_TRUE(topic_id.has_value());
        return topic_id.value();
    }

    ss::future<> wait_for_reconciliation() {
        auto topic_id = get_topic_id(nt);
        auto& meta = get_ct_app(n0).get_sharded_replicated_metastore()->local();

        for (int p = 0; p < num_partitions; ++p) {
            SCOPED_TRACE(
              fmt::format("Waiting for reconciliation on partition {}", p));
            model::topic_id_partition tp(topic_id, model::partition_id(p));
            RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&]() {
                return meta.get_offsets(tp).then(
                  [](auto result) { return result.has_value(); });
            });
        }
    }

    ss::future<> flush_metastore() {
        auto& meta = get_ct_app(n0).get_sharded_replicated_metastore()->local();
        auto flush_res = co_await meta.flush();
        RPTEST_REQUIRE_CORO(flush_res.has_value())
          << fmt::to_string(flush_res.error());
    }

    ss::future<> upload_controller_snapshot() {
        auto& app = *get_node_application(n0);
        auto& controller_stm = app.controller->get_controller_stm().local();
        auto raft0
          = app.partition_manager.local().get(model::controller_ntp)->raft();

        RPTEST_REQUIRE_EVENTUALLY_CORO(
          5s, [&] { return controller_stm.maybe_write_snapshot(); });

        auto& uploader = app.controller->metadata_uploader().value().get();
        retry_chain_node retry_node(never_abort, 30s, 1s);
        cluster::cloud_metadata::cluster_metadata_manifest manifest;
        manifest.cluster_uuid = app.storage.local().get_cluster_uuid().value();
        auto upload_res = co_await uploader.upload_next_metadata(
          raft0->confirmed_term(), manifest, retry_node);
        RPTEST_REQUIRE_CORO(
          upload_res == cluster::cloud_metadata::error_outcome::success);

        RPTEST_REQUIRE_CORO(
          manifest.metadata_id
          == cluster::cloud_metadata::cluster_metadata_id(0));
        RPTEST_REQUIRE_CORO(!manifest.controller_snapshot_path.empty());
    }

    void wipe_cluster() {
        for (size_t i = 0; i < 3; ++i) {
            auto n = model::node_id(i);
            instance(n)->shutdown();
            std::filesystem::remove_all(instance(n)->data_dir);
            remove_node_application(n);
        }
        for (size_t i = 0; i < 3; ++i) {
            add_node(fixture_cfg());
        }
        wait_for_all_members(5s).get();
    }

    ss::future<> initialize_recovery() {
        partition_ptr leader_p;
        co_await wait_for_leader(model::controller_ntp, &leader_p);
        auto leader_id = leader_p->raft()->self().id();

        auto& recovery_mgr = get_node_application(leader_id)
                               ->controller->get_cluster_recovery_manager()
                               .local();
        auto recover_err = co_await recovery_mgr.initialize_recovery(
          bucket_name, std::nullopt);
        RPTEST_REQUIRE_CORO(recover_err.has_value());
    }

    ss::future<> wait_for_recovery(
      std::chrono::seconds timeout, bool expect_failed = false) {
        auto& rcv_table = get_node_application(n0)
                            ->controller->get_cluster_recovery_table()
                            .local();
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          timeout, [&rcv_table] { return !rcv_table.is_recovery_active(); });
        auto current = rcv_table.current_recovery();
        RPTEST_REQUIRE_CORO(current.has_value());
        if (expect_failed) {
            EXPECT_EQ(current->get().stage, cluster::recovery_stage::failed);
        } else {
            EXPECT_EQ(current->get().stage, cluster::recovery_stage::complete);
        }
    }

protected:
    scoped_config cfg;
};

// Parameterized test fixture for tests that vary leadership stability.
class CloudTopicsClusterRecoveryParamsTest
  : public CloudTopicsClusterRecoveryTestBase
  , public ::testing::TestWithParam<test_params> {
public:
    void SetUp() override { CloudTopicsClusterRecoveryTestBase::SetUp(); }

    ss::future<> wait_for_recovery() {
        auto params = GetParam();
        auto timeout = (params.unstable_controller || params.unstable_metastore)
                         ? 120s
                         : 30s;
        return CloudTopicsClusterRecoveryTestBase::wait_for_recovery(timeout);
    }
};

// Non-parameterized test fixture.
class CloudTopicsClusterRecoveryTest
  : public CloudTopicsClusterRecoveryTestBase
  , public ::testing::Test {
public:
    void SetUp() override { CloudTopicsClusterRecoveryTestBase::SetUp(); }
};

TEST_P(CloudTopicsClusterRecoveryParamsTest, TestFullRecovery) {
    auto params = GetParam();

    // Create the cloud topic and produce data.
    ASSERT_NO_FATAL_FAILURE(create_cloud_topic().get());
    ASSERT_NO_FATAL_FAILURE(produce_some().get());

    vlog(test_log.info, "Waiting for reconciliation");
    ASSERT_NO_FATAL_FAILURE(wait_for_reconciliation().get());

    vlog(test_log.info, "Flushing metastore");
    ASSERT_NO_FATAL_FAILURE(flush_metastore().get());

    auto* orig_app0 = get_node_application(n0);
    auto& orig_topics = orig_app0->controller->get_topics_state().local();

    // Capture some metadata from the original cluster to validate after
    // recovery.
    const auto orig_uuid = orig_app0->storage.local().get_cluster_uuid();
    const auto orig_metastore_label = orig_topics
                                        .get_topic_cfg(model::l1_metastore_nt)
                                        ->properties.remote_label;
    ASSERT_TRUE(orig_metastore_label.has_value());
    const auto orig_tp_id = get_topic_id(nt);

    vlog(test_log.info, "Uploading controller snapshot");
    ASSERT_NO_FATAL_FAILURE(upload_controller_snapshot().get());

    vlog(test_log.info, "Wiping cluster");
    wipe_cluster();

    vlog(test_log.info, "Initializing recovery");
    ASSERT_NO_FATAL_FAILURE(initialize_recovery().get());

    // Start the leadership changes.
    bool stop = false;
    std::vector<ss::future<>> futs;
    auto stop_cleanup = ss::defer([&] {
        stop = true;
        ss::when_all_succeed(futs.begin(), futs.end()).get();
    });
    if (params.unstable_controller) {
        futs.emplace_back(leadership_change_fiber(stop, model::controller_ntp));
    }
    if (params.unstable_metastore) {
        model::ntp ntp{
          model::kafka_internal_namespace,
          model::l1_metastore_topic,
          model::partition_id{0}};
        wait_for_leader(ntp).get();
        futs.emplace_back(leadership_change_fiber(stop, ntp));
    }

    // Wait for recovery to complete.
    vlog(test_log.info, "Waiting for recovery to finish");
    ASSERT_NO_FATAL_FAILURE(wait_for_recovery().get());

    stop_cleanup.cancel();
    stop = true;
    ss::when_all_succeed(futs.begin(), futs.end()).get();

    vlog(test_log.info, "Waiting for controller leader");
    wait_for_leader(model::controller_ntp).get();

    // Validate the state after recovery.
    auto* rcv_app0 = get_node_application(n0);
    auto& rcv_topics = rcv_app0->controller->get_topics_state().local();

    const auto rcv_uuid = rcv_app0->storage.local().get_cluster_uuid();
    const auto rcv_metastore_label = rcv_topics
                                       .get_topic_cfg(model::l1_metastore_nt)
                                       ->properties.remote_label;
    const auto rcv_ct_label
      = rcv_topics.get_topic_cfg(nt)->properties.remote_label;
    const auto rcv_tp_id = get_topic_id(nt);
    const auto rcv_tp_count = rcv_topics.get_topic_cfg(nt)->partition_count;

    // The cluster UUID is new, but the metastore topic label and cloud topic
    // labels point at the original cluster, and topic IDs are preserved.
    EXPECT_NE(orig_uuid, rcv_uuid);
    EXPECT_EQ(orig_metastore_label, rcv_metastore_label);
    EXPECT_EQ(rcv_ct_label, orig_metastore_label);
    EXPECT_EQ(orig_tp_id, rcv_tp_id);
    EXPECT_EQ(num_partitions, rcv_tp_count);
    // TODO: check offsets once topic restore is implemented.

    // New topics have their labels pointing at the original cluster UUID too,
    // indicating that their metadata is in the metastore with that label.
    vlog(test_log.info, "Creating new topic");
    model::topic new_topic{"oolong"};
    create_cloud_topic(new_topic).get();
    const auto new_ct_label
      = rcv_topics.get_topic_cfg(nt)->properties.remote_label;
    EXPECT_EQ(orig_metastore_label, new_ct_label);
}

INSTANTIATE_TEST_SUITE_P(
  LeadershipVariants,
  CloudTopicsClusterRecoveryParamsTest,
  ::testing::Values(
    test_params{.unstable_controller = false, .unstable_metastore = false},
    test_params{.unstable_controller = true, .unstable_metastore = false},
    test_params{.unstable_controller = false, .unstable_metastore = true},
    test_params{.unstable_controller = true, .unstable_metastore = true}),
  [](const auto& info) { return fmt::format("{}", info.param); });

TEST_F(CloudTopicsClusterRecoveryTest, TestRecoveryWithExistingTopic) {
    ASSERT_NO_FATAL_FAILURE(create_cloud_topic().get());
    ASSERT_NO_FATAL_FAILURE(produce_some().get());

    vlog(test_log.info, "Waiting for reconciliation");
    ASSERT_NO_FATAL_FAILURE(wait_for_reconciliation().get());

    vlog(test_log.info, "Flushing metastore");
    ASSERT_NO_FATAL_FAILURE(flush_metastore().get());

    auto* orig_app0 = get_node_application(n0);
    auto& orig_topics = orig_app0->controller->get_topics_state().local();
    const auto orig_metastore_label = orig_topics
                                        .get_topic_cfg(model::l1_metastore_nt)
                                        ->properties.remote_label;
    ASSERT_TRUE(orig_metastore_label.has_value());

    vlog(test_log.info, "Uploading controller snapshot");
    ASSERT_NO_FATAL_FAILURE(upload_controller_snapshot().get());

    vlog(test_log.info, "Wiping cluster");
    wipe_cluster();

    vlog(test_log.info, "Creating topic before recovery");
    model::topic new_topic{"oolong"};
    create_cloud_topic(new_topic).get();

    vlog(test_log.info, "Initializing recovery");
    ASSERT_NO_FATAL_FAILURE(initialize_recovery().get());

    vlog(test_log.info, "Waiting for recovery to fail");
    ASSERT_NO_FATAL_FAILURE(
      wait_for_recovery(30s, /*expect_failed=*/true).get());
}
