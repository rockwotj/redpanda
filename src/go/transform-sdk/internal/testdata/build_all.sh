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
  GOARCH=wasm GOOS=wasip1 go build -o "$PKG.wasm" ./$PKG/transform.go
  echo "done ✔️"
done
echo "All packages built 🚀"
