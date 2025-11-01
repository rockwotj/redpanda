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

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "bytes/ioarray.h"
#include "bytes/iobuf.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/generator.hh>
#include <seastar/util/optimized_optional.hh>

namespace lsm::io {

// A file abstraction for reading sequentially through a file.
class sequential_file_reader {
public:
    sequential_file_reader() = default;
    sequential_file_reader(const sequential_file_reader&) = delete;
    sequential_file_reader& operator=(const sequential_file_reader&) = default;
    sequential_file_reader(sequential_file_reader&&) = delete;
    sequential_file_reader& operator=(sequential_file_reader&&) = delete;
    virtual ~sequential_file_reader() = default;

    // Read up to "n" bytes from the file.
    //
    // This will only return less than `n` if at the end of the file
    virtual ss::future<iobuf> read(size_t n) = 0;

    // Skip "n" bytes form the file.
    //
    // If EOF is reached, skipping will stop at the end of the file, and skip
    // will return without an error.
    virtual ss::future<> skip(size_t n) = 0;

    // Closes the file, must always be called, even if `read` or `skip` return
    // an error future.
    virtual ss::future<> close() = 0;

    // Print debug information about the file
    virtual fmt::iterator format_to(fmt::iterator) const = 0;
};

// A file abstraction for randomly reading the contents of a file.
class random_access_file_reader {
public:
    random_access_file_reader() = default;
    random_access_file_reader(const random_access_file_reader&) = delete;
    random_access_file_reader& operator=(const random_access_file_reader&)
      = delete;
    random_access_file_reader(random_access_file_reader&&) = delete;
    random_access_file_reader& operator=(random_access_file_reader&&) = delete;
    virtual ~random_access_file_reader() = default;

    // Read up to "n" bytes from the file starting at "offset".
    //
    // It's not valid to read outside the bounds of the file.
    virtual ss::future<ioarray> read(size_t offset, size_t n) = 0;

    // Closes the file, must always be called, even if `read` or `skip` return
    // an error future.
    virtual ss::future<> close() = 0;

    // Print debug information about the file
    virtual fmt::iterator format_to(fmt::iterator) const = 0;
};

// A file abstraction for sequential writing. The implementation must provide
// buffering since callers may append small fragments at a time to the file.
class sequential_file_writer {
public:
    sequential_file_writer() = default;
    sequential_file_writer(const sequential_file_writer&) = delete;
    sequential_file_writer& operator=(const sequential_file_writer&) = delete;
    sequential_file_writer(sequential_file_writer&&) = delete;
    sequential_file_writer& operator=(sequential_file_writer&&) = delete;
    virtual ~sequential_file_writer() = default;

    // Append the iobuf to the file.
    virtual ss::future<> append(iobuf) = 0;

    // Append the ioarray to the file.
    virtual ss::future<> append(ioarray) = 0;

    // Close the file, this ensures the file is properly written to persistent
    // storage (ie. flushed and fsync'd, etc).
    virtual ss::future<> close() = 0;

    // Print debug information about the file
    virtual fmt::iterator format_to(fmt::iterator) const = 0;
};

template<typename T>
using optional_pointer = ss::optimized_optional<std::unique_ptr<T>>;

// The persistence layer for long term file storage.
//
// This is intended to be a replacement for `leveldb::Env`,
// but only for the file portions of that interface.
//
// The APIs are structured slightly differently, as we want
// direct access for both cloud APIs and local filesystems.
//
// There is no concept of a "directory". All files are stored in a flat
// key/value namespace such that the `/` character is never used in a file name.
class persistence {
public:
    persistence() = default;
    persistence(const persistence&) = delete;
    persistence& operator=(const persistence&) = delete;
    persistence(persistence&&) = delete;
    persistence& operator=(persistence&&) = delete;
    virtual ~persistence() = default;

    // Create a reader that sequentially reads the file with the specified name.
    //
    // If the file does not exist then the implementation should return
    // `std::nullopt`.
    virtual ss::future<optional_pointer<sequential_file_reader>>
    open_sequential_reader(std::string_view name) = 0;

    // Create a reader that supports random access reads the file with the
    // specified name.
    //
    // If the file does not exist then the implementation should return
    // `std::nullopt`.
    virtual ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(std::string_view name) = 0;

    // Create a writer that writes to a new file with the specified name.
    //
    // Deletes any existing file with the same name and creates a new file.
    virtual ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(std::string_view name) = 0;

    // NOTE: These next two APIs are not currently needed, as we don't write log
    // files in this layer (instead we use the local raft log), and we
    // additionally don't write delta manifest updates. We also can't support
    // this API for cloud storage, so it'd really only be an optimization for
    // rapidly changing databases on local disk persistence.

    // // Returns true if the `open_appendable_sequential_writer` API is
    // // suppported.
    // //
    // // This API is used to append to manifest files delta operations, instead
    // // of writing only snapshots.
    // virtual bool supports_file_appends() const = 0;

    // // Create a writer that writes to a file with the specified name.
    // //
    // // If an existing file is present, the file is appended to. Otherwise, a
    // // new file is created.
    // virtual ss::future<std::unique_ptr<sequential_file_writer>>
    // open_appendable_sequential_writer(std::string_view name) = 0;

    // Write the string atomically to the persistence layer and specified name.
    //
    // This is used by the LSM tree to write CURRENT files, which are used to
    // point to the latest manifest file in the database. This must be written
    // atomically as to ensure crash recovery, this means the staging file, and
    // rename case in local disk, or just a normal PUT in cloud storage which
    // already provides atomic writes.
    //
    // The contents should be small (less than a KiB).
    virtual ss::future<>
    write_file_atomically(std::string_view name, std::string_view contents) = 0;

    // Remove a file. This is a noop if the file does not exist.
    virtual ss::future<> remove_file(std::string_view) = 0;

    // List all the files in the persistence layer.
    virtual ss::coroutine::experimental::generator<ss::sstring>
    list_files() = 0;

    // Closes the persistence layer, this must happen after all files are
    // closed.
    virtual ss::future<> close() = 0;
};

} // namespace lsm::io
