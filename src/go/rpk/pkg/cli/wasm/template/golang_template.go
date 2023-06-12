// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

import (
	"strings"
	"text/template"
)

const wasmMainFile = `
package main

import (
	"io"

	"github.com/rockwotj/redpanda/src/go/sdk"
)

func main() {
	redpanda.OnTransform(onTransform)
}

// This is an example of the "identity" transform that does nothing.
// You'll want to replace this with something that modifies the key
// or value.
func onTransform(e redpanda.TransformEvent) ([]redpanda.Record, error) {
	return []redpanda.Record{e.Record()}, nil
}
`

func WasmGoMain() string {
	return wasmMainFile
}

func ExecTemplate(filename string, source string, data interface{}) (string, error) {
	var tpl strings.Builder
	t, err := template.New(filename).Parse(source)
	if err != nil {
		return "", err
	}
	err = t.Execute(&tpl, data)
	return tpl.String(), err
}

// You can generate the sha via `go get`ting @branch
const wasmGoModFile = `module {{.}}

go 1.18
`

func WasmGoModule(name string) (string, error) {
	return ExecTemplate("go.mod", wasmGoModFile, name)
}

const wasmGoReadme = `
# Redpanda Golang WASM Transform

To get started you first need to have installed [tinygo].

You can get started by modifying the <code>transform.go</code> file
with your logic.

Once you're ready to test out your transform live you need to:

1. Make sure you have a container running via <code>rpk container start</code>
1. Run <code>rpk wasm build</code>
1. Run <code>rpk wasm deploy [topic]</code>
1. Then use <code>rpk topic produce [topic]</code> and <code>rpk topic consume [topic]</code> 
   to see your transformation live!

⚠️ At the moment the transform you deploy is applied on all topics in a cluster 
and is not persisted to disk, so if you restart your container you'll need to redeploy.

[tinygo]: https://tinygo.org/getting-started/install/
`

func WasmGoReadme() string {
	return wasmGoReadme
}
