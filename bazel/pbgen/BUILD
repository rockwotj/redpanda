load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "pbgen_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/bazel/pbgen",
    visibility = ["//visibility:private"],
    deps = [
        "//proto/redpanda/core:version_go_proto",
        "//proto/redpanda/pbgen:options_go_proto",
        "//proto/redpanda/pbgen:rpc_go_proto",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protodesc",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//reflect/protoregistry",
        "@org_golang_google_protobuf//types/descriptorpb",
        "@org_golang_google_protobuf//types/pluginpb",
    ],
)

go_binary(
    name = "pbgen",
    embed = [":pbgen_lib"],
    visibility = ["//visibility:public"],
)
