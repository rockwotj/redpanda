load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/lsm:__subpackages__"],
)

redpanda_cc_library(
    name = "footer",
    srcs = [
        "footer.cc",
    ],
    hdrs = [
        "footer.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/lsm/core:exceptions",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block:handle",
    ],
)
