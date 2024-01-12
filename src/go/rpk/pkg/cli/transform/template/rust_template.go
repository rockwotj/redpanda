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
	_ "embed"
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
)

//go:embed rust/main.rustsrc
var rustMainFile string

func WasmRustMain() string {
	return rustMainFile
}

const cargoTomlTemplate = `[package]
name = "%s"
version = "0.1.0"
edition = "2021"

[dependencies]
`

const rustDependencies = `redpanda-transform-sdk = %q
`

func WasmRustCargoConfig(name string) string {
	cargoToml := fmt.Sprintf(cargoTomlTemplate, name)
	v := version.Version()
	// Just let dev builds use the latest version and force usage of `cargo add redpanda-transform-sdk`,
	// otherwise fix the version to be the same as RPK
	if v.IsReleasedVersion() {
		// Rust versions drop the `v` prefix.
		cargoToml += fmt.Sprintf(rustDependencies, strings.TrimPrefix(v.Version, "v"))
	}
	return cargoToml
}

//go:embed rust/README.md
var wasmRustReadme string

func WasmRustReadme() string {
	return wasmRustReadme
}
