load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "string",
    srcs = [
        "string.cc",
    ],
    hdrs = [
        "string.h",
    ],
    include_prefix = "serde/json/detail",
    visibility = [
        ":__subpackages__",
        "//src/v/serde/json:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
