load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "immutable_btree",
    hdrs = [
        "immutable_btree.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
