load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "compression",
    srcs = ["compression.cc"],
    hdrs = ["compression.h"],
    implementation_deps = [
        "//src/v/compression",
    ],
    deps = [
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
