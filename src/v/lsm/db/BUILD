load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "table_builder",
    srcs = [
        "table_builder.cc",
    ],
    hdrs = [
        "table_builder.h",
    ],
    implementation_deps = [
        "//src/v/lsm/sst:builder",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iter",
    srcs = [
        "iter.cc",
    ],
    hdrs = [
        "iter.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "impl.cc",
    ],
    hdrs = [
        "impl.h",
    ],
    implementation_deps = [
        ":iter",
        ":table_builder",
        ":version_edit",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/core/internal:merging_iterator",
        "//src/v/lsm/sst:builder",
    ],
    deps = [
        ":memtable",
        ":table_cache",
        ":version_set",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:batch",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/ssx:condition_variable",
        "@seastar",
    ],
)

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
    name = "file_utils",
    srcs = [
        "file_utils.cc",
    ],
    hdrs = [
        "file_utils.h",
    ],
    deps = [
        ":version_edit",
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "@boost//:range",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version_set",
    srcs = [
        "version_set.cc",
    ],
    hdrs = [
        "version_set.h",
    ],
    implementation_deps = [
        ":file_utils",
        ":manifest_redpanda_proto",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:merging_iterator",
        "//src/v/lsm/core/internal:two_level_iterator",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:btree",
    ],
    deps = [
        ":table_cache",
        ":version_edit",
        ":weak_intrusive_list",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
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

proto_library(
    name = "manifest_proto",
    srcs = ["manifest.proto"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "manifest_redpanda_proto",
    protos = [":manifest_proto"],
)
