/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "utils/detailed_error.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <expected>
#include <ostream>
#include <string_view>

namespace cloud_topics::l1 {

class io;
class replicated_database;

// An object garbage collector that garbage collects based on object metadata
// in an database.
//
// Garbage collection is based only on object metadata that has been persisted
// to object storage, which ensures that subsequent restores of the database do
// not end up referring to missing objects.
class db_garbage_collector {
public:
    enum class errc {
        // The database needs reopening, e.g. term has changed, domain has been
        // restored.
        db_needs_reopen,

        // There was an error in interacting with cloud or in replicating data.
        // Likely transient, but still might be worth looking at.
        io_error,
    };
    using error = detailed_error<errc>;
    explicit db_garbage_collector(io* io);

    ss::future<std::expected<void, error>> remove_unreferenced_objects(
      replicated_database*, ss::abort_source*, size_t batch_size);

private:
    // Removes the given batch size worth of objects, evaluating objects
    // starting from the given object. Returns the next object that needs to be
    // evalulated, or std::nullopt if this batch finished all objects in the
    // database.
    ss::future<std::expected<std::optional<object_id>, error>>
    remove_unreferenced_batch(
      replicated_database*,
      ss::abort_source*,
      size_t batch_size,
      std::optional<object_id>);

    io* io_;
};

inline constexpr std::string_view
format_as(db_garbage_collector::errc e) {
    switch (e) {
    case db_garbage_collector::errc::db_needs_reopen:
        return "db_needs_reopen";
    case db_garbage_collector::errc::io_error:
        return "io_error";
    }
    return "unknown";
}

inline std::ostream& operator<<(std::ostream& o, db_garbage_collector::errc e) {
    return o << format_as(e);
}

} // namespace cloud_topics::l1
