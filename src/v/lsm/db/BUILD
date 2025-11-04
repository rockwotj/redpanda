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
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:batch",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
