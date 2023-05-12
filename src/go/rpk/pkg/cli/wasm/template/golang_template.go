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

`

func WasmGoMain() string {
	return wasmMainFile
}

const wasmGoModFile = `
module example.com/transform

go 1.18

require (
	github.com/rockwotj/redpanda/src/go/sdk wasmdev
)
`

func WasmGoModule() string {
	return wasmGoModFile
}

const wasmGoReadme = `
# Redpanda Golang WASM Transform

To get started you first need to have installed [tinygo].

You can find your main.go file

[tinygo]: https://tinygo.org/getting-started/install/
`

func WasmGoReadme() string {
	return wasmGoReadme
}
