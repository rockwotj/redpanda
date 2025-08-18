load(":llvm_headers.bzl", "copy_llvm_headers")

LLVM_TOOLCHAIN_REPO = "@current_llvm_toolchain_llvm"

copy_llvm_headers(
    name = "copied_llvm_includes",
    srcs = [LLVM_TOOLCHAIN_REPO + "//:all_includes"],
)

cc_library(
    name = "clang",
    srcs = [LLVM_TOOLCHAIN_REPO + "//:libclang"],
    hdrs = [":copied_llvm_includes"],
    includes = ["llvm_include"],
)

cc_library(
    name = "checks",
    srcs = ["example.cc"],
    copts = ["-fno-rtti"],
    deps = [":clang"],
)

cc_binary(
    name = "clang_tidy_plugin.so",
    copts = ["-fno-rtti"],
    # Add this shared library as data so that we can also load it at runtime
    data = [LLVM_TOOLCHAIN_REPO + "//:libclang"],
    linkopts = ["-Wl,--no-undefined", "-rdynamic"],
    linkshared = True,
    visibility = ["//bazel/clang_tidy:__pkg__"],
    deps = [":checks"],
)
