load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "parser_test",
    timeout = "short",
    srcs =
        [
            "parser_test.cc",
        ],
    cpu = 1,
    data = [
        "testdata/json-test-suite/sample.json",
    ],
    deps = [
        ":data",
        "//src/v/base",
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

redpanda_cc_bench(
    name = "parser_rpbench",
    timeout = "short",
    srcs = [
        "parser_bench.cc",
    ],
    cpu = 1,
    data = [
        "testdata/json-test-suite/sample.json",
    ],
    deps = [
        ":data",
        "//src/v/bytes:streambuf",
        "//src/v/json",
        "//src/v/serde/json:parser",
        "@boost//:test",
        "@rapidjson",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

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

redpanda_test_cc_library(
    name = "data",
    srcs = [
        "data.cc",
    ],
    hdrs = [
        "data.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
    ],
    include_prefix = "serde/json/tests",
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
