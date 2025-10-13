load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "w_tinylfu",
    hdrs = ["w_tinylfu.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "@boost//:intrusive",
        "@seastar",
    ],
)
