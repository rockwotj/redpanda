load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "kvstore_test",
    timeout = "short",
    srcs = [
        "kvstore_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "memtable_test",
    timeout = "short",
    srcs = [
        "memtable_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm:memtable",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
