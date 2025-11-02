load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "keys_test",
    timeout = "short",
    srcs = [
        "keys_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:keys",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "files_test",
    timeout = "short",
    srcs = [
        "files_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "iterator_test_harness",
    hdrs = ["iterator_test_harness.h"],
    visibility = ["//src/v/lsm:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@googletest//:gtest",
    ],
)
