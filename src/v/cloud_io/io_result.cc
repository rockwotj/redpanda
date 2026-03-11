/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/io_result.h"

namespace cloud_io {

std::ostream& operator<<(std::ostream& o, const download_result& r) {
    return o << format_as(r);
}

std::ostream& operator<<(std::ostream& o, const upload_result& r) {
    return o << format_as(r);
}

} // namespace cloud_io
