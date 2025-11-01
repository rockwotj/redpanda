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
        "//src/v/lsm/core/internal:keys",
        "@seastar",
    ],
)
