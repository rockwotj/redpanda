load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "json_checker_test",
    timeout = "short",
    srcs =
        [
            "json_checker_test.cc",
        ],
    cpu = 1,
    data = glob(["testdata/jsonchecker/*"]),
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)
