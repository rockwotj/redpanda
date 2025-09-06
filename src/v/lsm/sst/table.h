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

#include "absl/functional/function_ref.h"
#include "base/seastarx.h"
#include "lsm/core/iterator.h"
#include "lsm/core/keys.h"
#include "lsm/io/persistence.h"

#include <seastar/core/future.hh>

namespace lsm::sst {

// A table is a sorted map from string to string. Tables are immutable and
// persistent. A table does not need external synchronization to be used.
class table {
    class impl;

public:
    table(const table&) = delete;
    table& operator=(const table&) = delete;
    table(table&&) = default;
    table& operator=(table&&) = default;
    ~table();

    // Open the table that is stored in bytes [0..file_size) of "file", and read
    // the metadata entries necessary to allow retrieving data from the table.
    static ss::future<table>
    open(std::unique_ptr<io::random_access_file_reader> file, size_t file_size);

    // Returns a new iterator over the table contents.
    //
    // The result of create_iterator is initially invalid (caller must call one
    // of the seek* methods on the iterator before using it).
    std::unique_ptr<core::iterator> create_iterator();

    // Calls the function with the key/value pair with the entry found after a
    // call to `create_iterator()->seek(key)`. May not make such a call if the
    // filter policy says that key is not present.
    ss::future<> internal_get(
      core::internal_key_view key,
      absl::FunctionRef<ss::future<>(core::internal_key_view, iobuf)> fn);

private:
    explicit table(std::unique_ptr<impl>);
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::sst
