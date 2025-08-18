#!/usr/bin/env python3
import sys
import os
import subprocess
from pathlib import Path
from python.runfiles import Runfiles

def main():
    if len(sys.argv) < 3:
        print(
            f"Usage: {sys.argv[0]} <CLANG_TIDY_BIN> <FAKE_OUTPUT> <CONFIG> [ARGS...]"
        )
        sys.exit(1)

    clang_tidy_bin = sys.argv[1]
    fake_output = sys.argv[2]
    config_file = sys.argv[3]
    remaining_args = sys.argv[4:]

    # Bazel requires some kind of output file must be specified
    # so always create it
    Path(fake_output).touch(exist_ok=True)

    plugin_path = Path(Runfiles.Create().Rlocation("redpanda/bazel/clang_tidy/checks/clang_tidy_plugin.so"))
    assert plugin_path.exists(), f"Plugin not found at {plugin_path}"
    libclang_so_path = Path(Runfiles.Create().Rlocation("current_llvm_toolchain_llvm/lib/libclang.so"))
    assert libclang_so_path.exists(), f"libclang.so not found at {libclang_so_path}"
    # We have to resolve the symlink because the linker puts the version in the shared library, and bazel
    # only puts the symlink in the runfiles.
    ld_library_path = str(libclang_so_path.resolve(strict=True).parent)
    if "LD_LIBRARY_PATH" in os.environ:
        ld_library_path += ":" + os.environ["LD_LIBRARY_PATH"]
    clang_tidy_env = {**os.environ, "LD_LIBRARY_PATH": ld_library_path}

    try:
        checks = [
            clang_tidy_bin, f"--load={plugin_path}", "--checks=*", "--list-checks"
        ]
        c = subprocess.run(checks,
                           check=True,
                           capture_output=True,
                           text=True,
                           env=clang_tidy_env)
        print("Available checks:")
        print(c.stdout)
        if "redpanda-example-check" in c.stdout:
            print("SUCCESS: redpanda-example-check found!")
        else:
            print("ERROR: redpanda-example-check NOT found!")
            print("Checking for any redpanda checks...")
            if "redpanda" in c.stdout.lower():
                print("Found some redpanda-related checks")
            else:
                print("No redpanda-related checks found at all")
        verify_command = [
            clang_tidy_bin, f"--load={plugin_path}", f"--config-file={config_file}", "--verify-config"
        ]
        _ = subprocess.run(verify_command,
                           check=True,
                           capture_output=True,
                           text=True,
                           env=clang_tidy_env)

        run_command = [clang_tidy_bin, f"--load={plugin_path}", f"--config-file={config_file}"
                       ] + remaining_args

        _ = subprocess.run(run_command,
                           check=True,
                           capture_output=True,
                           text=True,
                           env=clang_tidy_env)

    except subprocess.CalledProcessError as e:
        print("clang-tidy command failed.", file=sys.stderr)
        if e.stdout:
            print("\n--- STDOUT ---", file=sys.stderr)
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print("\n--- STDERR ---", file=sys.stderr)
            print(e.stderr, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
