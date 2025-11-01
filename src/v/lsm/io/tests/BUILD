load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "persistence_test",
    timeout = "short",
    srcs = [
        "persistence_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/io:persistence",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)
