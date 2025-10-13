load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "w_tinylfu_test",
    timeout = "short",
    srcs = [
        "w_tinylfu_test.cc",
    ],
    deps = [
        "//src/v/lsm/utils:w_tinylfu",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "w_tinylfu_rpbench",
    srcs = [
        "w_tinylfu_bench.cc",
    ],
    deps = [
        "//src/v/lsm/utils:w_tinylfu",
        "//src/v/random:generators",
        "@abseil-cpp//absl/strings",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

# redpanda_cc_bench(
#     name = "s3_fifo_rpbench",
#     srcs = [
#         "s3_fifo_bench.cc",
#     ],
#     deps = [
#         "//src/v/random:generators",
#         "//src/v/utils:chunked_kv_cache",
#         "@abseil-cpp//absl/strings",
#         "@fmt",
#         "@seastar",
#         "@seastar//:benchmark",
#     ],
# )
