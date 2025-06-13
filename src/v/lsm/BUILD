load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "memtable",
    srcs = [
        "memtable.cc",
    ],
    hdrs = [
        "memtable.h",
    ],
    include_prefix = "lsm",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
