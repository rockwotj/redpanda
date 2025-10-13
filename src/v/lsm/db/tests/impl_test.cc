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

namespace io = lsm::io;

namespace {

class proxy_persistence : public io::persistence {
public:
    explicit proxy_persistence(io::persistence* p)
      : _p(p) {}

    ss::future<io::optional_pointer<io::sequential_file_reader>>
    open_sequential_reader(std::string_view name) override {
        return _p->open_sequential_reader(name);
    }

    ss::future<io::optional_pointer<io::random_access_file_reader>>
    open_random_access_reader(std::string_view name) override {
        return _p->open_random_access_reader(name);
    }

    ss::future<std::unique_ptr<io::sequential_file_writer>>
    open_sequential_writer(std::string_view name) override {
        return _p->open_sequential_writer(name);
    }

    ss::future<> write_file_atomically(
      std::string_view name, std::string_view contents) override {
        return _p->write_file_atomically(name, contents);
    }

    ss::future<> remove_file(std::string_view file) override {
        return _p->remove_file(file);
    }

    ss::coroutine::experimental::generator<ss::sstring> list_files() override {
        return _p->list_files();
    }

    ss::future<> close() override { co_return; };

private:
    io::persistence* _p;
};

class ImplTest : public testing::Test {
public:
    void SetUp() override {
        _options = ss::make_lw_shared<lsm::internal::options>({
          .level_zero_slowdown_writes_trigger = 4,
          .level_zero_stop_writes_trigger = 6,
          .write_buffer_size = 256_KiB,
          .level_one_compaction_trigger = 2,
          .max_file_size = 2_MiB,
        });
        _persistence = lsm::io::make_memory_persistence();
        _db = lsm::db::impl::open(
                _options,
                std::make_unique<proxy_persistence>(_persistence.get()))
                .get();
    }

    void TearDown() override {
        _db->close().get();
        _persistence->close().get();
    }

    void write_at_least(size_t size) {
        lsm::internal::write_batch batch;
        auto seqno = _db->max_applied_seqno();
        while (batch.memory_usage() < size) {
            auto key = lsm::internal::key::encode({
              .key = random_generators::gen_alphanum_max_distinct(100'000),
              .seqno = ++seqno,
            });
            auto value = iobuf::from(
              random_generators::gen_alphanum_string(1_KiB));
            _shadow.insert_or_assign(
              ss::sstring(key.user_key()), value.share());
            batch.put(key, value.share());
        }
        _db->apply(std::move(batch)).get();
    }

    testing::AssertionResult matches_shadow() {
        auto iter = _db->create_iterator().get();
        auto it = _shadow.begin();
        std::vector<std::string> errors;
        for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
            if (it == _shadow.end()) {
                errors.emplace_back("extra elements");
                break;
            }
            if (*it != std::make_pair(iter->key().user_key(), iter->value())) {
                errors.push_back(
                  fmt::format(
                    "expected key {}, got key {}, keys equal {}, values equal "
                    "{}",
                    it->first,
                    iter->key(),
                    it->first == iter->key().user_key(),
                    it->second == iter->value()));
            }
            ++it;
        }
        if (it != _shadow.end()) {
            errors.emplace_back("missing elements");
        }
        if (errors.empty()) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure()
               << fmt::format("{}", fmt::join(errors, "\n"));
    }

    void restart() {
        _db->close().get();
        _db = lsm::db::impl::open(
                _options,
                std::make_unique<proxy_persistence>(_persistence.get()))
                .get();
    }

    auto max_applied_seqno() { return _db->max_applied_seqno(); }
    auto max_persisted_seqno() { return _db->max_persisted_seqno(); }

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
    std::unique_ptr<lsm::io::persistence> _persistence;
    std::unique_ptr<lsm::db::impl> _db;
};

TEST_F(ImplTest, MemtableIsFlushed) {
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return list_files().then(
          [](const auto& files) { return files.size() > 0; });
    });
}

TEST_F(ImplTest, Recovery) {
    write_at_least(512_KiB);
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _db->flush().get();
    EXPECT_EQ(max_applied_seqno(), max_persisted_seqno());
    restart();
    EXPECT_TRUE(matches_shadow());
}

TEST_F(ImplTest, Randomized) {
#ifndef NDEBUG
    int rounds = 100;
#else
    int rounds = 1000;
#endif
    for (int i = 0; i < rounds; ++i) {
        write_at_least(512_KiB);
        EXPECT_TRUE(matches_shadow());
    }
}

} // namespace
