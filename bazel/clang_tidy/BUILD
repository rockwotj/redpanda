load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "clang_tidy",
    srcs = ["clang_tidy.py"],
    data = ["//bazel/clang_tidy/checks:clang_tidy_plugin.so"],
    deps = ["@rules_python//python/runfiles"],
)
