#!/bin/bash

for PKG in identity setup-panic transform-error transform-panic
do
  tinygo build -target wasi -opt=z \
    -panic trap -scheduler none \
    -o "$PKG.wasm" "$PKG/transform.go"
done
