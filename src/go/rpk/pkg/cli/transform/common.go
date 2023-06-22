// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package transform

import (
	"os"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type WasmLang string

const (
	WasmLangTinygo WasmLang = "tinygo"
)

var AllWasmLangs = []string{"tinygo"}

type WasmProjectConfig struct {
	Name        string            `yaml:"name"`
	InputTopic  string            `yaml:"input-topic"`
	OutputTopic string            `yaml:"output-topic"`
	Language    WasmLang          `yaml:"language"`
	Env         map[string]string `yaml:"env,omitempty"`
}

var configFileName = "transform.yaml"

func marshalConfig(c WasmProjectConfig) ([]byte, error) {
	return yaml.Marshal(c)
}

func saveCfg(fs afero.Fs, c WasmProjectConfig) error {
	b, err := yaml.Marshal(&c)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, configFileName, b, os.FileMode(0o644))
}

func loadCfg(fs afero.Fs) (WasmProjectConfig, error) {
	b, err := afero.ReadFile(fs, configFileName)
	var c WasmProjectConfig
	if err != nil {
		return c, err
	}
	err = yaml.Unmarshal(b, &c)
	return c, err
}
