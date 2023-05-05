#!/bin/bash

clang -target wasm32 \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
  -o identity_transform.wasm identity_transform.cc 

clang -target wasm32 \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
  -o bang_transform.wasm bang_transform.cc 

clang -target wasm32 \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
 -DOPENAI_API_KEY="\"$OPENAI_API_KEY\"" \
  -o ai_transform.wasm ai_transform.cc 

