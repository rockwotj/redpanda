#!/bin/bash

clang -target wasm32 \
  -Wl,--no-entry -nostdlib -std=c++20 \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
  -o identity_transform.wasm identity_transform.cc 
