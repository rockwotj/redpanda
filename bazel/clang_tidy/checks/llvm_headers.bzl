def _is_relevant_header(filepath):
    included = ["llvm", "clang-tidy", "clang"]
    for inc in included:
        if filepath.startswith("include/" + inc):
            return True
    return False

def _copy_llvm_headers_impl(ctx):
    """Implementation of the copy_filegroup rule."""
    outputs = []

    for src in ctx.attr.srcs[0].files.to_list():
        # Get the relative path from the workspace root
        # This preserves the directory structure
        relative_path = src.short_path

        # Handle external dependencies (they start with ../)
        if relative_path.startswith("../"):
            # Remove the ../external_repo/ prefix for external files
            parts = relative_path.split("/")
            relative_path = "/".join(parts[2:])

        if not _is_relevant_header(relative_path):
            continue

        out = ctx.actions.declare_file("llvm_" + relative_path)
        outputs.append(out)

        # Copy the file
        ctx.actions.run_shell(
            inputs = [src],
            outputs = [out],
            command = "cp -f '%s' '%s'" % (src.path, out.path),
            mnemonic = "CopyFile",
            progress_message = "Copying %s" % src.short_path,
            use_default_shell_env = False,
        )

    return [DefaultInfo(files = depset(outputs))]

copy_llvm_headers = rule(
    implementation = _copy_llvm_headers_impl,
    attrs = {
        "srcs": attr.label_list(
            mandatory = True,
            allow_files = True,
            doc = "Files to copy",
        ),
    },
    doc = "Copies LLVM headers to the current workspace, preserving directory structure.",
)
