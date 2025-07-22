load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/batch_cache:__pkg__",
    "//src/v/cloud_topics/core:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/frontend:__pkg__",
    "//src/v/cloud_topics/level_zero:__subpackages__",
    "//src/v/cloud_topics/read_path:__pkg__",
    "//src/v/cloud_topics/read_path/tests:__pkg__",
    "//src/v/cloud_topics/tests:__pkg__",
    "//src/v/cloud_topics/throttler:__pkg__",
    "//src/v/cloud_topics/throttler/tests:__pkg__",
    "//src/v/kafka/data:__pkg__",
])

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "errc.h",
        "types.h",
    ],
    include_prefix = "cloud_topics",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/serde",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "placeholder",
    hdrs = [
        "dl_placeholder.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":types",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "dl_version",
    hdrs = ["dl_version.h"],
    include_prefix = "cloud_topics",
    deps = ["//src/v/utils:named_type"],
)

redpanda_cc_library(
    name = "dl_snapshot",
    srcs = [
        "dl_snapshot.cc",
    ],
    hdrs = [
        "dl_snapshot.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":dl_version",
        "//src/v/container:fragmented_vector",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "data_plane_api",
    hdrs = [
        "data_plane_api.h",
    ],
    include_prefix = "cloud_topics",
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:extent_meta",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "app",
    srcs = [
        "app.cc",
    ],
    hdrs = [
        "app.h",
    ],
    include_prefix = "cloud_topics",
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/level_zero/reconciler:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:extent_meta",
        "//src/v/cloud_topics/level_one/domain:domain_supervisor",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_plane_impl",
    srcs = [
        "data_plane_impl.cc",
    ],
    hdrs = [
        "data_plane_impl.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/core:read_pipeline",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/level_zero/batcher",
        "//src/v/cloud_topics/level_zero/reconciler",
        "//src/v/cloud_topics/read_path:fetch_handler",
        "//src/v/cloud_topics/throttler",
        "//src/v/model",
        "//src/v/storage",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//src/v/redpanda:__pkg__"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "extent_meta",
    hdrs = [
        "extent_meta.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":types",
        "//src/v/model",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "object_utils",
    srcs = [
        "object_utils.cc",
    ],
    hdrs = [
        "object_utils.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":cluster_services_interface",
        ":types",
        "//src/v/cloud_storage_clients",
        "//src/v/ssx:sformat",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "cluster_services_interface",
    hdrs = [
        "cluster_services.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ephemeral_cluster_services",
    hdrs = [
        "ephemeral_cluster_services.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_services_interface",
        "@seastar",
    ],
)
