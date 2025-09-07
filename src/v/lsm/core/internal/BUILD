load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "iterator",
    srcs = ["iterator.cc"],
    hdrs = [
        "iterator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":keys",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "keys",
    srcs = ["keys.cc"],
    hdrs = [
        "keys.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)
