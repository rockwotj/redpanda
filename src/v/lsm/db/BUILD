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
        "//src/v/lsm/core/internal:keys",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "table_cache",
    srcs = [
        "table_cache.cc",
    ],
    hdrs = [
        "table_cache.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:reader",
        "//src/v/utils:chunked_kv_cache",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version_set",
    srcs = [
        "version_edit.cc",
    ],
    hdrs = [
        "version_edit.h",
    ],
    implementation_deps = [
        "//src/v/utils:to_string",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/io:persistence",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "weak_intrusive_list",
    hdrs = ["weak_intrusive_list.h"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
