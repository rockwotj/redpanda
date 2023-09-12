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
	"strings"
	"text/template"
)

//go:embed golang/transform.gosrc
var wasmMainFile string

func WasmGoMain() string {
	return wasmMainFile
}

func execTemplate(filename string, source string, data interface{}) (string, error) {
	var tpl strings.Builder
	t, err := template.New(filename).Parse(source)
	if err != nil {
		return "", err
	}
	err = t.Execute(&tpl, data)
	return tpl.String(), err
}

const wasmGoModFile = `module {{.}}

go 1.20
`

func WasmGoModule(name string) (string, error) {
	return execTemplate("go.mod", wasmGoModFile, name)
}

//go:embed golang/README.md
var wasmGoReadme string

func WasmGoReadme() string {
	return wasmGoReadme
}
