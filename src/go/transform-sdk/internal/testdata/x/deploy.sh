#!/usr/bin/env bash

rpk topic create json avro -r 3 -p 576

rpk wasm deploy x.wasm --name=x --input-topic=json --output-topic=avro
