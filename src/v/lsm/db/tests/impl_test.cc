/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "gtest/gtest.h"
#include "lsm/core/internal/batch.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/io/memory_persistence.h"
#include "random/generators.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

namespace {

class ImplTest : public testing::Test {
public:
    void SetUp() override {
        _options = ss::make_lw_shared<lsm::internal::options>(
          {.write_buffer_size = 1_MiB});
        auto persistence = lsm::io::make_memory_persistence();
        _persistence = persistence.get();
        _db = lsm::db::impl::open(_options, std::move(persistence)).get();
    }

    void TearDown() override { _db->close().get(); }

    void write_at_least(size_t size) {
        lsm::internal::write_batch batch;
        while (batch.memory_usage() < size) {
            auto key = lsm::internal::key::encode({
              .key = random_generators::gen_alphanum_string(64),
              .seqno = ++_db->max_applied_seqno(),
            });
            auto value = iobuf::from(
              random_generators::gen_alphanum_string(32_KiB));
            _shadow.insert_or_assign(
              ss::sstring(key.user_key()), value.share());
            batch.put(key, value.share());
        }
        _db->apply(std::move(batch)).get();
    }

    testing::AssertionResult matches_shadow() {
        auto iter = _db->create_iterator().get();
        std::map<ss::sstring, iobuf> actual;
        for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
            actual.emplace(iter->key().user_key(), iter->value());
        }
        if (actual == _shadow) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure();
    }

    ss::future<std::vector<ss::sstring>> list_files() {
        auto gen = _persistence->list_files();
        std::vector<ss::sstring> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

protected:
    std::map<ss::sstring, iobuf> _shadow;
    ss::lw_shared_ptr<lsm::internal::options> _options;
    lsm::io::persistence* _persistence = nullptr;
    std::unique_ptr<lsm::db::impl> _db;
};

TEST_F(ImplTest, MemtableIsFlushed) {
    EXPECT_TRUE(matches_shadow());
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return list_files().then(
          [](const auto& files) { return files.size() > 0; });
    });
}

} // namespace
