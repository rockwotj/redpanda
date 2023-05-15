// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

const wasmMainFile = `
package main

import (
	"io"

	"redpanda.com/wasm-sdk"
)

func main() {
	redpanda.OnTransform(onTransform)
}

// Temporary buffer so that GC isn't invoked so much!
var buf = make([]byte, 4096)

// This is an example of the "identity" transform that does nothing.
// You'll want to replace this with something that modifies the key
// or value.
func onTransform(e redpanda.TransformEvent) error {
	output, err := redpanda.CreateOutputRecord()
	if err != nil {
		return err
	}

	// copy over the key
	_, err = io.CopyBuffer(output.Key(), e.Record().Key(), buf)
	if err != nil {
		return err
	}
		
	// copy over the value
	_, err = io.CopyBuffer(output.Value(), e.Record().Value(), buf)
	if err != nil {
		return err
	}

	// copy over the headers
	for _, k := range(e.Record().Headers().Keys()) {
		v := e.Record().Headers().Get(k)
		err = output.AppendHeader(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}
`

func WasmGoMain() string {
	return wasmMainFile
}

// You can generate the sha via `go get`ting @branch
const wasmGoModFile = `
module example.com/transform

go 1.18

require (
	github.com/rockwotj/redpanda/src/go/sdk 
)
`

func WasmGoModule() string {
	return wasmGoModFile
}

const wasmGoReadme = `
# Redpanda Golang WASM Transform

To get started you first need to have installed [tinygo].

You can get started by modifying the <code>transform.go</code> file
with your logic.

Once you're ready to test out your transform live you need to:

1. Make sure you have a container running via <code>rpk container start</code>
1. Run <code>rpk wasm build</code>
1. Run <code>rpk wasm deploy</code>
1. Then use <code>rpk topic produce</code> and <code>rpk topic consume</code> 
   to see your transformation live!

⚠️ At the moment the transform you deploy is applied on all topics in a cluster 
and is not persisted to disk, so if you restart your container you'll need to redeploy.

[tinygo]: https://tinygo.org/getting-started/install/
`

func WasmGoReadme() string {
	return wasmGoReadme
}
