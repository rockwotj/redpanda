load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "compression",
    srcs = ["compression.cc"],
    hdrs = ["compression.h"],
    implementation_deps = [
        ":exceptions",
        "//src/v/compression",
    ],
    deps = [
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "exceptions",
    hdrs = ["exceptions.h"],
    deps = ["@fmt"],
)

redpanda_cc_library(
    name = "lookup_result",
    hdrs = ["lookup_result.h"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
