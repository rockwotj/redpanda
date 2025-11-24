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
