load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/lsm:__subpackages__"],
)

redpanda_cc_library(
    name = "builder",
    srcs = [
        "builder.cc",
    ],
    hdrs = [
        "builder.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:keys",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "handle",
    srcs = ["handle.cc"],
    hdrs = [
        "handle.h",
    ],
    deps = [
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "contents",
    srcs = [
        "contents.cc",
    ],
    hdrs = [
        "contents.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
    ],
    deps = [
        ":handle",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    implementation_deps = [
        "//src/v/hashing:xx",
    ],
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core:keys",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "block",
    srcs = [
        "block.cc",
    ],
    hdrs = [
        "block.h",
    ],
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/lsm/core:iterator",
        "//src/v/lsm/core:keys",
        "@seastar",
    ],
)
