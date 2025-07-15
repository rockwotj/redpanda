load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "admin_proto",
    srcs = ["admin.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core:version_proto",
        "//proto/redpanda/pbgen:options_proto",
        "//proto/redpanda/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "admin_redpanda_proto",
    protos = [":admin_proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/core:version_redpanda_proto"],
)
