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

redpanda_cc_library(
    name = "table_cache",
    srcs = [
        "table_cache.cc",
    ],
    hdrs = [
        "table_cache.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/sst:reader",
        "//src/v/ssx:work_queue",
        "//src/v/utils:mutex",
        "//src/v/utils:s3_fifo",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:block_cache",
        "@abseil-cpp//absl/functional:function_ref",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version_edit",
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
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "@abseil-cpp//absl/container:fixed_array",
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
