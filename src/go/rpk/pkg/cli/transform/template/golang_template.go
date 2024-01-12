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
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
)

type goSdkVersion struct {
	major string
	full  string
}

func computeGoSdkVersion() goSdkVersion {
	v := version.Version()
	if !v.IsReleasedVersion() {
		// Use the current year as the major version
		return goSdkVersion{
			major: time.Now().Format("06"),
			full:  "",
		}
	}
	parts := strings.SplitN(v.Version, ".", 2)
	return goSdkVersion{
		major: parts[0],
		full:  v.Version,
	}
}

//go:embed golang/transform.gosrc
var wasmGolangMainFile string

func WasmGoMain() string {
	return fmt.Sprintf(wasmGolangMainFile, computeGoSdkVersion().major)
}

const wasmGoModFile = `module %s

go 1.20
`

const requiredGoModules = `
require github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/%s %s
`

func WasmGoModule(name string) string {
	modFile := fmt.Sprintf(wasmGoModFile, name)
	// Just let dev builds use the latest version and force usage of `go mod tidy`,
	// otherwise fix the version to be the same as RPK
	v := computeGoSdkVersion()
	if v.full != "" {
		modFile += fmt.Sprintf(requiredGoModules, v.major, v.full)
	}
	return modFile
}

//go:embed golang/README.md
var wasmGoReadme string

func WasmGoReadme() string {
	return wasmGoReadme
}
