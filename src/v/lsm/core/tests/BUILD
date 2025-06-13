load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "keys_test",
    timeout = "short",
    srcs = [
        "keys_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core:keys",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
