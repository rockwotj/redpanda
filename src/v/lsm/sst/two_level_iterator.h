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

#include "bytes/iobuf.h"
#include "lsm/core/iterator.h"

#include <seastar/util/noncopyable_function.hh>

namespace lsm::sst {

// A function to convert an index_iter value into an iterator over the contents
// of the corresponding block.
using block_function = ss::noncopyable_function<
  ss::future<std::unique_ptr<core::iterator>>(iobuf index_value)>;

// Return a new two level iterator. A two-level iterator contains an index
// iterator whose values point ot a sequence of blocks where each block is
// itself a sequence of key,value pairs. The returned iterator yields the
// concatenation of all key/value pairs in the sequence of blocks.
std::unique_ptr<core::iterator> create_two_level_iterator(
  std::unique_ptr<core::iterator> index_iter, block_function block_fn);

} // namespace lsm::sst
