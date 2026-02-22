load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduler",
    srcs = [
        "scheduler.cc",
    ],
    hdrs = [
        "scheduler.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_impl",
        "//src/v/raft",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "db",
    hdrs = [
        "db.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "db_impl",
    srcs = [
        "db_impl.cc",
    ],
    hdrs = [
        "db_impl.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/crypto",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:persistence",
        "//src/v/model:batch_builder",
        "//src/v/model:batch_compression",
        "//src/v/random:time_jitter",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
    ],
    deps = [
        ":db",
        "//src/v/cluster",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/ssx:mutex",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manager",
    srcs = [
        "manager.cc",
    ],
    hdrs = [
        "manager.h",
    ],
    implementation_deps = [
        ":db_impl",
        ":logger",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":db",
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:work_queue",
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
    implementation_deps = [
        ":manager",
        ":scheduler",
        "//src/v/cluster",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:sharded_service_container",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)
