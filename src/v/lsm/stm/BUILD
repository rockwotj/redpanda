load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "stm",
    srcs = [
        "key_index_stm.cc",
    ],
    hdrs = [
        "key_index_stm.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/model",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/lsm",
        "//src/v/raft",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "factory",
    srcs = [
        "key_index_stm_factory.cc",
    ],
    hdrs = [
        "key_index_stm_factory.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":stm",
        "//src/v/cluster:state_machine_registry",
    ],
)
