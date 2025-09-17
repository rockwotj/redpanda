load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

package(
    default_visibility = ["//visibility:public"],
)

proto_library(
    name = "common_proto",
    srcs = ["common/v1/common.proto"],
    import_prefix = "opentelemetry/proto",
    strip_import_prefix = "",
    deps = ["//proto/redpanda/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "common_redpanda_proto",
    include_prefix = "opentelemetry/proto/common/v1",
    protos = [":common_proto"],
)

proto_library(
    name = "resource_proto",
    srcs = ["resource/v1/resource.proto"],
    import_prefix = "opentelemetry/proto",
    strip_import_prefix = "",
    deps = [
        ":common_proto",
        "//proto/redpanda/pbgen:options_proto",
    ],
)

redpanda_proto_library(
    name = "resource_redpanda_proto",
    include_prefix = "opentelemetry/proto/resource/v1",
    protos = [":resource_proto"],
    deps = [":common_redpanda_proto"],
)

TELEMETRY_TYPES = [
    "logs",
    "metrics",
    "trace",
]

[
    (
        proto_library(
            name = telemetry + "_proto",
            srcs = ["{0}/v1/{0}.proto".format(telemetry)],
            import_prefix = "opentelemetry/proto",
            strip_import_prefix = "",
            deps = [
                ":common_proto",
                ":resource_proto",
                "//proto/redpanda/pbgen:options_proto",
            ],
        ),
        redpanda_proto_library(
            name = telemetry + "_redpanda_proto",
            include_prefix = "opentelemetry/proto/{}/v1".format(telemetry),
            protos = [":{}_proto".format(telemetry)],
            deps = [
                ":common_redpanda_proto",
                ":resource_redpanda_proto",
            ],
        ),
        proto_library(
            name = telemetry + "_service_proto",
            srcs = ["collector/{0}/v1/{0}_service.proto".format(telemetry)],
            import_prefix = "opentelemetry/proto",
            strip_import_prefix = "",
            deps = [
                ":{}_proto".format(telemetry),
                "//proto/redpanda/pbgen:options_proto",
            ],
        ),
        redpanda_proto_library(
            name = telemetry + "_service_redpanda_proto",
            include_prefix = "opentelemetry/proto/collector/{0}/v1".format(telemetry),
            protos = [":{}_service_proto".format(telemetry)],
            deps = [
                ":{}_redpanda_proto".format(telemetry),
            ],
        ),
    )
    for telemetry in TELEMETRY_TYPES
]
