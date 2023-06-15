#!/bin/bash

PKGS=( 
  identity
  setup-panic
  transform-error
  transform-panic
  schema-registry
)

for PKG in "${PKGS[@]}"
do
  echo "Building $PKG..."
  tinygo build -target wasi -opt=z \
    -panic print -scheduler none \
    -o "$PKG.wasm" ./$PKG
  echo "done ✔️"
done
echo "All packages built 🚀"
