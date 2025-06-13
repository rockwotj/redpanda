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
    include_prefix = "lsm/sst",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/compression",
        "//src/v/hashing:crc32c",
        "//src/v/lsm/block:builder",
        "//src/v/lsm/block:filter",
        "@seastar",
    ],
)
