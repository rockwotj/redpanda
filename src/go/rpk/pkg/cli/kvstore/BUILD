load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "kvstore",
    srcs = [
        "delete.go",
        "get.go",
        "kvstore.go",
        "put.go",
        "scan.go",
        "types.go",
        "utils.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/kvstore",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go//pkg/kerr",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
    ],
)
