load(":llvm_headers.bzl", "copy_llvm_headers", "copy_llvm_static_libs")

# I'm not sure if there is a better way to reference the toolchain
LLVM_TOOLCHAIN_REPO = "@@toolchains_llvm++llvm+current_llvm_toolchain_llvm"

copy_llvm_headers(
    name = "copied_llvm_includes",
    srcs = [LLVM_TOOLCHAIN_REPO + "//:all_includes"],
)

copy_llvm_static_libs(
    name = "copied_llvm_static_libs",
    srcs = [LLVM_TOOLCHAIN_REPO + "//:"],
)

cc_library(
    name = "clang",
    srcs = [":copied_llvm_static_libs"],
    hdrs = [":copied_llvm_includes"],
    includes = ["llvm_include"],
)

cc_library(
    name = "checks",
    srcs = ["example.cc"],
    deps = [":clang"],
)

cc_binary(
    name = "clang_tidy_plugin.so",
    linkshared = True,
    visibility = ["//bazel/clang_tidy:__pkg__"],
    deps = [":checks"],
)
