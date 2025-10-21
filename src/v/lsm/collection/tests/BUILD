load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "immutable_btree_test",
    timeout = "short",
    srcs = [
        "immutable_btree_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/collection:immutable_btree",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
