load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "contents_test",
    timeout = "short",
    srcs = [
        "contents_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block:contents",
        "//src/v/test_utils:gtest",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "filter_test",
    timeout = "short",
    srcs = [
        "filter_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block:contents",
        "//src/v/lsm/block:filter",
        "//src/v/lsm/core:keys",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iterator_test",
    timeout = "short",
    srcs = [
        "iterator_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block",
        "//src/v/lsm/block:builder",
        "//src/v/lsm/block:contents",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
