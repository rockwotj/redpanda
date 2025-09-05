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

#include "lsm/io/disk_persistence.h"

#include "base/units.h"
#include "lsm/io/persistence.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

namespace lsm::io {

namespace {

class disk_seq_file_reader : public sequential_file_reader {
public:
    explicit disk_seq_file_reader(ss::input_stream<char> stream)
      : _stream(std::move(stream)) {}

    ss::future<iobuf> read(size_t n) override {
        iobuf buf;
        while (n > 0) {
            auto tmp_buf = co_await _stream.read_up_to(std::min(n, 128_KiB));
            if (tmp_buf.empty()) {
                break;
            }
            n -= tmp_buf.size();
            buf.append(std::make_unique<iobuf::fragment>(std::move(tmp_buf)));
        }
        co_return buf;
    }
    ss::future<> skip(size_t n) override { co_await _stream.skip(n); }
    ss::future<> close() override { return _stream.close(); }

private:
    ss::input_stream<char> _stream;
};

class disk_file_reader : public random_access_file_reader {
public:
    explicit disk_file_reader(ss::file file)
      : _file(std::move(file)) {}

    ss::future<ioarray> read(size_t offset, size_t n) override {
        size_t memory_alignment = _file.memory_dma_alignment();
        size_t disk_alignment = _file.disk_read_dma_alignment();
        size_t adjusted_offset = ss::align_down(offset, disk_alignment);
        size_t offset_delta = offset - adjusted_offset;
        auto array = ioarray::aligned(
          memory_alignment, ss::align_up(n + offset_delta, disk_alignment));
        size_t amt = co_await _file.dma_read(adjusted_offset, array.as_iovec());
        if (amt != array.size()) {
            throw std::runtime_error(
              fmt::format(
                "short read: failed to read {} bytes from block at offset {}, "
                "got: "
                "{}",
                array.size(),
                adjusted_offset,
                amt));
        }
        co_return array.share(offset_delta, n);
    }

    ss::future<> close() override { return _file.close(); }

private:
    ss::file _file;
};

class disk_seq_file_writer : public sequential_file_writer {
public:
    explicit disk_seq_file_writer(ss::output_stream<char> stream)
      : _stream(std::move(stream)) {}

    ss::future<> append(iobuf buf) override {
        for (const auto& frag : buf) {
            co_await _stream.write(frag.get(), frag.size());
        }
    }
    ss::future<> append(ioarray array) override {
        for (const auto& frag : array.as_iovec()) {
            co_await _stream.write(
              static_cast<const char*>(frag.iov_base), frag.iov_len);
        }
    }
    ss::future<> close() override {
        co_await _stream.flush();
        co_await _stream.close();
    }

private:
    ss::output_stream<char> _stream;
};

class impl : public persistence {
public:
    explicit impl(std::filesystem::path root)
      : _root(std::move(root)) {}

    ~impl() override = default;

    ss::future<optional_pointer<sequential_file_reader>>
    open_sequential_reader(std::string_view name) override {
        try {
            auto file = ss::open_file_dma(
              path(name).native(), ss::open_flags::ro);
            auto stream = co_await ss::with_file_close_on_failure(
              std::move(file), [](ss::file f) {
                  return ss::make_file_input_stream(std::move(f));
              });
            std::unique_ptr<sequential_file_reader> ptr;
            ptr = std::make_unique<disk_seq_file_reader>(std::move(stream));
            co_return ptr;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                co_return std::nullopt;
            }
            throw;
        }
    }

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(std::string_view name) override {
        try {
            auto file = co_await ss::open_file_dma(
              path(name).native(), ss::open_flags::ro);
            std::unique_ptr<random_access_file_reader> ptr;
            ptr = std::make_unique<disk_file_reader>(std::move(file));
            co_return ptr;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                co_return std::nullopt;
            }
            throw;
        }
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(std::string_view name) override {
        auto file = ss::open_file_dma(
          path(name).native(),
          ss::open_flags::create | ss::open_flags::exclusive
            | ss::open_flags::rw | ss::open_flags::truncate);
        auto stream = co_await ss::with_file_close_on_failure(
          std::move(file),
          [](ss::file f) { return ss::make_file_output_stream(std::move(f)); });
        co_return std::make_unique<disk_seq_file_writer>(std::move(stream));
    }

    ss::future<> remove_file(std::string_view name) override {
        try {
            co_await ss::remove_file(name);
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw;
            }
        }
    }

    ss::coroutine::experimental::generator<ss::sstring> list_files() override {
        auto dir = co_await ss::open_directory(_root.native());
        auto generator = dir.experimental_list_directory();
        while (auto entry = co_await generator()) {
            co_yield entry->name;
        }
    }

    ss::future<> close() override { co_return; }

private:
    std::filesystem::path path(std::string_view name) { return _root / name; }

    std::filesystem::path _root;
};

} // namespace

ss::future<std::unique_ptr<persistence>>
open_disk_persistence(std::filesystem::path directory) {
    co_await ss::recursive_touch_directory(directory.native());
    co_return std::make_unique<impl>(std::move(directory));
}

} // namespace lsm::io
