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

for PKG in "${PKGS[@]}"
do
  echo "Building $PKG..."
  ~/.local/rpk/buildpacks/tinygo/bin/tinygo -target wasi -opt=z \
    -panic print -scheduler none \
    -o "$PKG.wasm" ./$PKG
  echo "done ✔️"
done
echo "All packages built 🚀"
