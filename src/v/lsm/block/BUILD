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
    include_prefix = "lsm/block",
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
    include_prefix = "lsm/block",
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
    include_prefix = "lsm/block",
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
    include_prefix = "lsm/block",
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
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
    include_prefix = "lsm/block",
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/lsm/core:iterator",
        "//src/v/lsm/core:keys",
        "@seastar",
    ],
)
