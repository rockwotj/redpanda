load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "numeric_test",
    timeout = "short",
    srcs = [
        "numeric_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/random:generators",
        "//src/v/serde/json/detail:numeric",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "string_test",
    timeout = "short",
    srcs = [
        "string_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/json/detail:string",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

redpanda_cc_bench(
    name = "string_rpbench",
    timeout = "short",
    srcs = [
        "string_bench.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/serde/json/detail:string",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
