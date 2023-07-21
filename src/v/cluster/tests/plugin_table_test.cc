/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <boost/concept/detail/has_constraints.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <initializer_list>
#include <optional>
#include <stdexcept>
#define BOOST_TEST_MODULE plugin_table

#include "cluster/plugin_table.h"

#include <boost/test/unit_test.hpp>

namespace {
using plugin_map
  = absl::flat_hash_map<cluster::transform_id, cluster::transform_metadata>;

struct metadata_spec {
    ss::sstring name;
    ss::sstring input;
    std::vector<ss::sstring> output;
    absl::flat_hash_map<ss::sstring, ss::sstring> env;
};
cluster::transform_id id(int id) { return cluster::transform_id(id); }
absl::flat_hash_set<cluster::transform_id> ids(std::initializer_list<int> ids) {
    absl::flat_hash_set<cluster::transform_id> output;
    for (int id : ids) {
        output.emplace(id);
    }
    return output;
}
cluster::transform_name name(ss::sstring n) {
    return cluster::transform_name(std::move(n));
}
model::topic_namespace topic(ss::sstring t) {
    return {model::kafka_namespace, model::topic(t)};
}
cluster::transform_metadata make_meta(metadata_spec spec) {
    std::vector<model::topic_namespace> outputs;
    outputs.reserve(spec.output.size());
    for (const auto& name : spec.output) {
        outputs.emplace_back(topic(name));
    }
    return {
      .name = cluster::transform_name(spec.name),
      .input_topic = topic(spec.input),
      .output_topics = std::move(outputs),
      .environment = spec.env,
    };
}
} // namespace

BOOST_AUTO_TEST_CASE(plugin_table_mutations) {
    cluster::plugin_table table;

    auto wheat_millwheel = make_meta(
      {.name = "millwheel", .input = "wheat", .output = {"flour"}});
    auto cow = make_meta({.name = "cow", .input = "grass", .output = {"milk"}});
    auto lawn = make_meta(
      {.name = "lawn", .input = "water", .output = {"grass", "weeds"}});
    auto corn_millwheel = make_meta(
      {.name = "millwheel", .input = "corn", .output = {"cornmeal"}});

    table.upsert_transform(id(1), wheat_millwheel);
    table.upsert_transform(id(2), cow);
    table.upsert_transform(id(3), lawn);
    // Names must be unique
    BOOST_CHECK_THROW(
      table.upsert_transform(id(4), corn_millwheel), std::logic_error);

    table.upsert_transform(id(1), corn_millwheel);

    plugin_map want{
      {id(1), corn_millwheel},
      {id(2), cow},
      {id(3), lawn},
    };
    BOOST_CHECK_EQUAL(table.all_transforms(), want);
    table.remove_transform(name("cow"));
    want.erase(id(2));
    BOOST_CHECK_EQUAL(table.all_transforms(), want);
    table.reset_transforms(want);
    BOOST_CHECK_EQUAL(table.all_transforms(), want);
    want.erase(id(1));
    auto goat = make_meta(
      {.name = "goat", .input = "grass", .output = {"milk"}});
    want.emplace(id(4), goat);
    table.reset_transforms(want);
    BOOST_CHECK_EQUAL(table.all_transforms(), want);
}

BOOST_AUTO_TEST_CASE(plugin_table_notifications) {
    cluster::plugin_table table;
    absl::flat_hash_set<cluster::transform_id> notifications;
    auto notif_id = table.register_for_updates([&](auto id) {
        auto [it, inserted] = notifications.emplace(id);
        // To ensure we don't get duplicate notifications, make sure there are
        // always imports and we just clear the notifications after assertions
        BOOST_CHECK(inserted);
    });
    auto miracle = make_meta(
      {.name = "miracle", .input = "water", .output = {"wine"}});
    auto optimus_prime = make_meta(
      {.name = "optimus_prime", .input = "semi", .output = {"hero"}});
    auto optimus_prime2 = make_meta(
      {.name = "optimus_prime",
       .input = "semi",
       .output = {"hero"},
       .env = {{"allspark", "enabled"}}});
    auto parser = make_meta(
      {.name = "parser", .input = "text", .output = {"ast"}});
    auto compiler = make_meta(
      {.name = "compiler", .input = "awesomelang", .output = {"assembly"}});

    table.upsert_transform(id(1), miracle);
    BOOST_CHECK_EQUAL(notifications, ids({1}));
    notifications.clear();

    table.upsert_transform(id(2), optimus_prime);
    BOOST_CHECK_EQUAL(notifications, ids({2}));
    notifications.clear();

    table.remove_transform(miracle.name);
    BOOST_CHECK_EQUAL(notifications, ids({1}));
    notifications.clear();

    table.upsert_transform(id(3), miracle);
    table.upsert_transform(id(4), parser);
    BOOST_CHECK_EQUAL(notifications, ids({3, 4}));
    notifications.clear();

    table.reset_transforms(plugin_map({
      // changed
      {id(2), optimus_prime2},
      // unchanged
      {id(3), miracle},
      // removed id(4)
      // added 5
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
      {id(5), compiler},
    }));
    BOOST_CHECK_EQUAL(notifications, ids({2, 4, 5}));
    notifications.clear();

    table.unregister_for_updates(notif_id);
    table.remove_transform(miracle.name);
    BOOST_CHECK(notifications.empty());
}

BOOST_AUTO_TEST_CASE(plugin_table_topic_indexes) {
    const static std::optional<cluster::transform_metadata> missing
      = std::nullopt;
    const static std::optional<cluster::transform_id> missing_id = std::nullopt;
    cluster::plugin_table table;

    auto millwheel = make_meta(
      {.name = "millwheel", .input = "wheat", .output = {"flour"}});
    table.upsert_transform(id(1), millwheel);
    auto cow = make_meta({.name = "cow", .input = "grass", .output = {"milk"}});
    table.upsert_transform(id(2), cow);
    auto lawn = make_meta(
      {.name = "lawn", .input = "water", .output = {"grass", "weeds"}});
    table.upsert_transform(id(3), lawn);
    auto miller = make_meta(
      {.name = "miller", .input = "corn", .output = {"flour", "cornmeal"}});
    table.upsert_transform(id(4), miller);
    BOOST_CHECK_EQUAL(table.find_by_name("lawn"), std::make_optional(lawn));
    BOOST_CHECK_EQUAL(
      table.find_id_by_name("miller"), std::make_optional(id(4)));
    BOOST_CHECK_EQUAL(table.find_by_name("law"), missing);
    BOOST_CHECK_EQUAL(table.find_id_by_name("miter"), missing_id);
    BOOST_CHECK_EQUAL(table.find_by_id(id(2)), std::make_optional(cow));
    BOOST_CHECK_EQUAL(table.find_by_id(id(99)), missing);
    BOOST_CHECK_EQUAL(
      table.find_by_input_topic(topic("grass")), plugin_map({{id(2), cow}}));
    BOOST_CHECK_EQUAL(
      table.find_by_output_topic(topic("flour")),
      plugin_map({{id(1), millwheel}, {id(4), miller}}));
    BOOST_CHECK_EQUAL(table.find_by_input_topic(topic("waldo")), plugin_map());
}
