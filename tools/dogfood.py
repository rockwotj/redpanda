#!/usr/bin/env python3

import argparse
import subprocess
import json
import sys

BASE_DOCKER_IMAGE = "us-central1-docker.pkg.dev/rp-byoc-tyler/wasm-feature-branch/redpanda"

def tag_docker_image(args):
    output = json.loads(subprocess.check_output(["docker", "inspect", "localhost/redpanda:dev"]))
    arch = output[0]["Architecture"]
    tag = f"{BASE_DOCKER_IMAGE}:{arch}"
    subprocess.check_call(["docker", "tag", "localhost/redpanda:dev", tag])
    subprocess.check_call(["docker", "push", tag])

def create_docker_manifest(args):
    output = json.loads(subprocess.check_output(["docker", "inspect", "localhost/redpanda:dev"]))
    arch = output[0]["Architecture"]
    tags = []
    for arch in ["amd64", "arm64"]:
        tags.append("--amend")
        tags.append(f"{BASE_DOCKER_IMAGE}:{arch}")
    subprocess.check_call(["docker", "manifest", "create", f"{BASE_DOCKER_IMAGE}:dev", *tags])
    subprocess.check_call(["docker", "manifest", "push", f"{BASE_DOCKER_IMAGE}:dev"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    parser_foo = subparsers.add_parser('tag_docker_image')
    parser_foo.set_defaults(func=tag_docker_image)
    parser_foo = subparsers.add_parser('create_docker_manifest')
    parser_foo.set_defaults(func=create_docker_manifest)
    args = parser.parse_args()
    if not hasattr(args, 'func'):
            print('Unrecognized command', file=sys.stderr)
            parser.print_help()
            exit(1)
    args.func(args)
