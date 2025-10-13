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

#include "lsm/io/memory_persistence.h"

#include "base/format_to.h"
#include "lsm/core/exceptions.h"

#include <seastar/core/coroutine.hh>

#include <map>
#include <memory>

namespace lsm::io {

namespace {

struct memory_file_state {
    ss::sstring filename;
    iobuf data;
    int32_t open_read_handles = 0;
    int32_t open_write_handles = 0;

    int32_t open_handles() const {
        return open_read_handles + open_write_handles;
    }
};

class memory_sequential_file_reader : public sequential_file_reader {
public:
    explicit memory_sequential_file_reader(
      ss::lw_shared_ptr<memory_file_state> state)
      : _state(std::move(state)) {
        if (_state->open_write_handles > 0) {
            throw io_error_exception(
              "unable to open new readable file with open write handles: {}",
              _state->open_handles());
        }
        ++_state->open_read_handles;
    }
    ~memory_sequential_file_reader() override {
        vassert(_closed, "files must be closed before destructing");
    };

    ss::future<iobuf> read(size_t n) override {
        auto off = std::min(_offset, _state->data.size_bytes());
        auto max_len = _state->data.size_bytes() - off;
        auto len = std::min(max_len, n);
        _offset += len;
        co_return _state->data.share(off, len);
    }

    ss::future<> skip(size_t n) override {
        _offset += n;
        co_return;
    }

    ss::future<> close() override {
        if (_closed) {
            throw io_error_exception("double close of file");
        }
        _closed = true;
        --_state->open_read_handles;
        co_return;
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(
          it,
          "{{file={}, size={}, current_offset={}}}",
          _state->filename,
          _state->data.size_bytes(),
          _offset);
    }

private:
    size_t _offset = 0;
    bool _closed = false;
    ss::lw_shared_ptr<memory_file_state> _state;
};

class memory_random_access_file_reader : public random_access_file_reader {
public:
    explicit memory_random_access_file_reader(
      ss::lw_shared_ptr<memory_file_state> state)
      : _state(std::move(state)) {
        if (_state->open_write_handles > 0) {
            throw io_error_exception(
              "unable to open new readable file with open write handles: {}",
              _state->open_handles());
        }
        ++_state->open_read_handles;
    }

    ~memory_random_access_file_reader() override {
        vassert(_closed, "files must be closed before destructing");
    }

    ss::future<ioarray> read(size_t offset, size_t n) override {
        if ((offset + n) > _state->data.size_bytes()) {
            throw io_error_exception(
              "tried to read out of bounds of the file: "
              "{{offset:{},length:{},file_size:{}}}",
              offset,
              n,
              _state->data.size_bytes());
        }
        co_return ioarray::copy_from(_state->data.share(offset, n));
    }

    ss::future<> close() override {
        if (_closed) {
            throw io_error_exception("double close of file");
        }
        _closed = true;
        --_state->open_read_handles;
        co_return;
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(
          it,
          "{{file={}, size={}}}",
          _state->filename,
          _state->data.size_bytes());
    }

private:
    bool _closed = false;
    ss::lw_shared_ptr<memory_file_state> _state;
};

class memory_sequential_file_writer : public sequential_file_writer {
public:
    explicit memory_sequential_file_writer(
      ss::lw_shared_ptr<memory_file_state> state)
      : _state(std::move(state)) {
        if (_state->open_handles() > 0) {
            throw io_error_exception(
              "unable to open new writable file with open handles: {}",
              _state->open_handles());
        }
        ++_state->open_write_handles;
        _state->data.clear();
    }
    ~memory_sequential_file_writer() override {
        vassert(_closed, "files must be closed before destructing");
    }
    ss::future<> append(iobuf buf) override {
        _state->data.append(std::move(buf));
        co_return;
    }
    ss::future<> append(ioarray array) override {
        _state->data.append(array.as_iobuf());
        co_return;
    }
    ss::future<> close() override {
        if (_closed) {
            throw io_error_exception("double close of file");
        }
        _closed = true;
        --_state->open_write_handles;
        co_return;
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{file={}}}", _state->filename);
    }

private:
    bool _closed = false;
    ss::lw_shared_ptr<memory_file_state> _state;
};

class impl : public persistence {
public:
    ss::future<optional_pointer<sequential_file_reader>>
    open_sequential_reader(std::string_view name) override {
        auto it = _data.find(ss::sstring(name));
        std::unique_ptr<sequential_file_reader> ptr;
        if (it != _data.end()) {
            ptr = std::make_unique<memory_sequential_file_reader>(it->second);
        }
        co_return ptr;
    }

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(std::string_view name) override {
        auto it = _data.find(ss::sstring(name));
        std::unique_ptr<random_access_file_reader> ptr;
        if (it != _data.end()) {
            ptr = std::make_unique<memory_random_access_file_reader>(
              it->second);
        }
        co_return ptr;
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(std::string_view name) override {
        auto key = std::string(name);
        auto it = _data.try_emplace(
          ss::sstring(name),
          ss::make_lw_shared<memory_file_state>(ss::sstring(name)));
        co_return std::make_unique<memory_sequential_file_writer>(
          it.first->second);
    }

    ss::future<> write_file_atomically(
      std::string_view name, std::string_view contents) override {
        auto writer = co_await open_sequential_writer(name);
        co_await writer->append(iobuf::from(contents)).finally([&writer] {
            return writer->close();
        });
    }

    ss::future<> remove_file(std::string_view name) override {
        auto it = _data.find(ss::sstring(name));
        if (it == _data.end()) {
            co_return;
        }
        if (it->second->open_handles() != 0) {
            throw io_error_exception(
              "unable to remove file {}, there are still open handles", name);
        }
        _data.erase(it);
    }

    ss::coroutine::experimental::generator<ss::sstring> list_files() override {
        auto it = _data.begin();
        while (it != _data.end()) {
            auto key = it->first;
            co_yield key;
            it = _data.upper_bound(key);
        }
    }

    ss::future<> close() override {
        _closed = true;
        for (const auto& [file, state] : _data) {
            vassert(
              state->open_handles() == 0,
              "tried to close with open handles on file {}",
              file);
        }
        co_return;
    }

    ~impl() override { vassert(_closed, "persistence not properly closed"); }

private:
    bool _closed = false;
    std::map<ss::sstring, ss::lw_shared_ptr<memory_file_state>> _data;
};

} // namespace

std::unique_ptr<persistence> make_memory_persistence() {
    return std::make_unique<impl>();
}
} // namespace lsm::io
