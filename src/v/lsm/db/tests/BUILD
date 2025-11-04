load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "memtable_test",
    timeout = "short",
    srcs = [
        "memtable_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:batch",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal/tests:iterator_test_harness",
        "//src/v/lsm/db:memtable",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "weak_intrusive_list_test",
    timeout = "short",
    srcs = [
        "weak_intrusive_list_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/db:weak_intrusive_list",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
