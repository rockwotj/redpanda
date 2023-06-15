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
  pushd $PKG
  tinygo build -target wasi -opt=z \
    -panic print -scheduler none \
    -o "$PKG.wasm"
  echo "done ✔️"
  popd
done
echo "All packages built 🚀"
