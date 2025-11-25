load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "manager",
    srcs = [
        "manager.cc",
    ],
    hdrs = [
        "manager.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_impl",
        "//src/v/raft",
    ],
    deps = [
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "db",
    srcs = [
        "db.cc",
    ],
    hdrs = [
        "db.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:persistence",
        "//src/v/model:batch_builder",
        "//src/v/ssx:future_util",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/utils:mutex",
        "@seastar",
    ],
)
