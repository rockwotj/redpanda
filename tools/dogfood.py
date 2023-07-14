#!/usr/bin/env python3

import argparse
import subprocess
import json
import sys
import pathlib
import os

BASE_DOCKER_IMAGE = "us-central1-docker.pkg.dev/rp-byoc-tyler/wasm-feature-branch/redpanda"

def release_redpanda(args):
    env = {**os.environ}
    env["TARGETS"] = "redpanda rp_util"
    subprocess.check_call(["task", "rp:clean-pkg", "rp:build-docker-image", "BUILD_TYPE=release", "PKG_FORMATS=deb"], env=env)
    output = json.loads(subprocess.check_output(["docker", "inspect", "localhost/redpanda:dev"]))
    arch: str = output[0]["Architecture"]
    tag = f"{BASE_DOCKER_IMAGE}:{arch}"
    subprocess.check_call(["docker", "tag", "localhost/redpanda:dev", tag])
    subprocess.check_call(["docker", "push", tag])
    build_dir = pathlib.Path(__file__).parent.parent / "vbuild/release/clang/dist/debian/"
    deb_files = [path for path in build_dir.glob("*.deb") if 'dbgsym' not in str(path)]
    assert len(deb_files) == 1, f"Expected only a single deb file, found: {deb_files}"
    subprocess.check_call(["gcloud", "artifacts", "apt", "upload", "wasm-feature-branch-apt", f"--source={deb_files[0]}"])

def release_rpk(args):
    output = subprocess.check_output(["gh", "release", "list", "--repo", "rockwotj/redpanda", "--limit", "1"], text=True)
    latest_release = int(output.split("\t")[0].split("-")[1])
    build_dir = pathlib.Path(__file__).parent.parent / "vbuild/go"
    os.chdir(build_dir)
    for (goos, goarch) in [("linux", "amd64"), ("darwin", "arm64"), ("linux", "arm64"), ("darwin", "amd64")]:
        subprocess.check_call(["task", "rpk:build", f"GOOS={goos}", f"GOARCH={goarch}"])
        subprocess.check_call(["zip", "-r", f"rpk-{goos}-{goarch}", f"{goos}/{goarch}"])
    subprocess.check_call(["gh", "release", "create",
                           "--repo", "rockwotj/redpanda",
                           f"wasmdev-{latest_release + 1}",
                           "rpk-linux-amd64.zip", "rpk-darwin-arm64.zip", "rpk-darwin-amd64.zip", "rpk-linux-arm64.zip"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    parser_foo = subparsers.add_parser('release_rpk')
    parser_foo.set_defaults(func=release_rpk)
    parser_foo = subparsers.add_parser('release_redpanda')
    parser_foo.set_defaults(func=release_redpanda)
    args = parser.parse_args()
    if not hasattr(args, 'func'):
            print('Unrecognized command', file=sys.stderr)
            parser.print_help()
            exit(1)
    args.func(args)
