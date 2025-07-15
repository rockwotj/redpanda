load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "version_proto",
    srcs = ["version.proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "version_redpanda_proto",
    protos = [":version_proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "version_go_proto",
    importpath = "github.com/redpanda-data/redpanda/proto/redpanda/core",
    protos = [":version_proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/pbgen:options_go_proto"],
)

cc_proto_library(
    name = "version_cc_proto",
    visibility = ["//visibility:public"],
    deps = [
        ":version_proto",
    ],
)
