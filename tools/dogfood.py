#!/usr/bin/env python3

import argparse
import subprocess
import json
import sys
import pathlib
import os

BASE_DOCKER_IMAGE = "us-central1-docker.pkg.dev/rp-byoc-tyler/wasm-feature-branch/redpanda"

def build_docker_image(args):
    subprocess.check_call(["task", "rp:build-docker-image", "BUILD_TYPE=release", "PKG_FORMATS=deb"])

def tag_docker_image(args):
    output = json.loads(subprocess.check_output(["docker", "inspect", "localhost/redpanda:dev"]))
    arch: str = output[0]["Architecture"]
    tag = f"{BASE_DOCKER_IMAGE}:{arch}"
    subprocess.check_call(["docker", "tag", "localhost/redpanda:dev", tag])
    subprocess.check_call(["docker", "push", tag])

def release_rpk(args):
    output = subprocess.check_output(["gh", "release", "list", "--repo", "rockwotj/redpanda", "--limit", "1"], text=True)
    latest_release = int(output.split("\t")[0].split("-")[1])
    for (goos, goarch) in [("linux", "amd64"), ("darwin", "arm64")]:
        subprocess.check_call(["task", "rpk:build", f"GOOS={goos}", f"GOARCH={goarch}"])
    build_dir = pathlib.Path(__file__).parent.parent / "vbuild/go"
    os.chdir(build_dir)
    subprocess.check_call(["zip", "-r", "rpk-linux-amd64", "linux"])
    subprocess.check_call(["zip", "-r", "rpk-darwin-arm64", "darwin"])
    subprocess.check_call(["gh", "release", "create",
                           "--repo", "rockwotj/redpanda",
                           f"wasmdev-{latest_release + 1}",
                           "rpk-linux-amd64.zip", "rpk-darwin-arm64.zip"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    parser_foo = subparsers.add_parser('tag_docker_image')
    parser_foo.set_defaults(func=tag_docker_image)
    parser_foo = subparsers.add_parser('release_rpk')
    parser_foo.set_defaults(func=release_rpk)
    parser_foo = subparsers.add_parser('build_docker_image')
    parser_foo.set_defaults(func=build_docker_image)
    args = parser.parse_args()
    if not hasattr(args, 'func'):
            print('Unrecognized command', file=sys.stderr)
            parser.print_help()
            exit(1)
    args.func(args)
