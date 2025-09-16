load("@protobuf//bazel:proto_library.bzl", "proto_library")

package(
    default_visibility = ["//visibility:public"],
)

proto_library(
    name = "common_proto",
    srcs = ["common/v1/common.proto"],
    import_prefix = "opentelemetry/proto",
    strip_import_prefix = "",
)

proto_library(
    name = "resource_proto",
    srcs = ["resource/v1/resource.proto"],
    import_prefix = "opentelemetry/proto",
    strip_import_prefix = "",
    deps = [":common_proto"],
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
                "resource_proto",
                ":common_proto",
            ],
        ),
        proto_library(
            name = telemetry + "_service_proto",
            srcs = ["collector/{0}/v1/{0}_service.proto".format(telemetry)],
            import_prefix = "opentelemetry/proto",
            strip_import_prefix = "",
            deps = [":{}_proto".format(telemetry)],
        ),
    )
    for telemetry in TELEMETRY_TYPES
]
