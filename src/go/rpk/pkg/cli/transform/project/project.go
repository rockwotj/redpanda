// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package project

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

type Config struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description,omitempty"`
	InputTopic  string            `yaml:"input-topic"`
	OutputTopic string            `yaml:"output-topic"`
	Language    WasmLang          `yaml:"language"`
	Env         map[string]string `yaml:"env,omitempty"`
}

var ConfigFileName = "transform.yaml"

func MarshalConfig(c Config) ([]byte, error) {
	return yaml.Marshal(c)
}

func SaveCfg(fs afero.Fs, c Config) error {
	b, err := yaml.Marshal(&c)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, ConfigFileName, b, os.FileMode(0o644))
}
func UnmarshalConfig(b []byte) (c Config, err error) {
	err = yaml.Unmarshal(b, &c)
	return c, err
}

func LoadCfg(fs afero.Fs) (Config, error) {
	b, err := afero.ReadFile(fs, ConfigFileName)
	if err != nil {
		return Config{}, err
	}
	return UnmarshalConfig(b)
}
