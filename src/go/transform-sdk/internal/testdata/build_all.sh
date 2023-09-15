#!/bin/bash

PKGS=(
  identity
  setup-panic
  transform-error
  transform-panic
  schema-registry
  wasi
  poison-pill
)

for PKG in "${PKGS[@]}"; do
  echo "Building $PKG..."
  # Build using the buildpack, you need to install rpk and initialize
  # a project for this to work first.
  GOROOT=~/Workspace/redpanda2/vbuild/go/1.21.0 GOARCH=wasm GOOS=wasip1 ~/.local/rpk/buildpacks/tinygo/bin/tinygo build -o "$PKG.wasm" -target wasi ./$PKG/transform.go
  echo "done ✔️"
done
echo "All packages built 🚀"
