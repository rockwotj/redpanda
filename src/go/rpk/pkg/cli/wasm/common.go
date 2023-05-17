// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type WasmLang string

const (
	WasmLangGo WasmLang = "Golang"
)

var AllWasmLangs = []string{"Golang"}

type WasmProjectConfig struct {
	Name string `yaml:"name"`
	Topic string `yaml:"topic"`
	Language WasmLang `yaml:"language"`
}

func marshalConfig(c WasmProjectConfig) ([]byte, error) {
	return yaml.Marshal(c)
}
func loadCfg(fs afero.Fs) (WasmProjectConfig, error) {
	b, err := afero.ReadFile(fs, "redpandarc.yaml")
	var c WasmProjectConfig
	if err != nil {
		return c, err
	}
	err = yaml.Unmarshal(b, &c)
	return c, err
}
