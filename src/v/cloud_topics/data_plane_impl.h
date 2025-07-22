/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/data_plane_api.h"
#include "model/fundamental.h"

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace cluster {
class partition_manager;
} // namespace cluster

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_storage {
class cache;
}

namespace storage {
class api;
}

namespace experimental::cloud_topics {

class cluster_services;

ss::shared_ptr<data_plane_api> make_data_plane(
  seastar::sharded<cluster::partition_manager>*,
  seastar::sharded<cloud_io::remote>*,
  seastar::sharded<cloud_storage::cache>*,
  cloud_storage_clients::bucket_name bucket,
  seastar::sharded<storage::api>* log_manager,
  std::unique_ptr<cluster_services> cluster_services);

} // namespace experimental::cloud_topics
