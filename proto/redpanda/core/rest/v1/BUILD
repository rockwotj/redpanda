load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "kvstore_proto",
    srcs = [
        "kvstore.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
    ],
)

redpanda_proto_library(
    name = "kvstore_redpanda_proto",
    protos = [
        ":kvstore_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [],
)
