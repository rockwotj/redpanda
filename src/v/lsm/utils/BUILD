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

redpanda_cc_library(
    name = "immutable_btree",
    hdrs = ["immutable_btree.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
