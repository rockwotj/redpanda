load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "persistence",
    hdrs = ["persistence.h"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_persistence",
    srcs = ["memory_persistence.cc"],
    hdrs = ["memory_persistence.h"],
    deps = [
        ":persistence",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "disk_persistence",
    srcs = ["disk_persistence.cc"],
    hdrs = ["disk_persistence.h"],
    deps = [
        ":persistence",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)
