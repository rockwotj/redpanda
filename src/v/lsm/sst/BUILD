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
        "//src/v/bytes:iostream",
        "//src/v/hashing:crc32c",
        "//src/v/lsm/block:builder",
        "//src/v/lsm/block:filter",
        "//src/v/lsm/block:handle",
        "//src/v/lsm/core:compression",
        "//src/v/lsm/core:keys",
        "@seastar",
    ],
)
