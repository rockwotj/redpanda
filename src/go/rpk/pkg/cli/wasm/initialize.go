// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/wasm/template"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type transformProject struct {
	Name  string
	Path  string
	Lang  WasmLang
	Topic string
}

func newInitializeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create a template project for Wasm engine",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			name, path, err := determineTransformName(fs)
			out.MaybeDie(err, "unable to determine transform name: %v", err)
			lang, err := determineTransformLang()
			out.MaybeDie(err, "unable to determine transform language: %v", err)
			adm, err := kafka.NewAdmin(fs, p, cfg)
			topic, err := determineTransformTopic(cmd.Context(), adm)
			out.MaybeDie(err, "unable to determine transform topic: %v", err)
			err = executeGenerate(fs, transformProject{Name: name, Path: path, Lang: lang, Topic: topic})
			out.MaybeDie(err, "unable to generate all manifest files: %v", err)
		},
	}
	return cmd
}

func determineTransformTopic(ctx context.Context, adm *kadm.Client) (topic string, err error) {
	topics, err := adm.ListTopics(ctx)
	var topicNames []string
	if err != nil {
		topicNames = topics.Names()
	}
	qs := []*survey.Question{
		{
			Name: "topic",
			Prompt: &survey.Input{
				Message: "select a topic to transform:",
				Suggest: func(toComplete string) (ret []string) {
					for _, t := range topicNames {
						if strings.HasPrefix(t, toComplete) {
							ret = append(ret, t)
						}
					}
					return
				},
			},
		},
	}
	err = survey.Ask(qs, &topic)
	return
}

func determineTransformLang() (lang WasmLang, err error) {
	qs := []*survey.Question{
		{
			Name: "lang",
			Prompt: &survey.Select{
				Message: "select a language:",
				Options: AllWasmLangs,
			},
		},
	}
	var langVal string
	err = survey.Ask(qs, &langVal)
	return WasmLang(langVal), err
}

func determineTransformName(fs afero.Fs) (name string, path string, err error) {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = ""
	}
	qs := []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "transform name:",
				Default: cwd,
			},
			Validate: func(val interface{}) error {
				// the reflect value of the result
				value := reflect.ValueOf(val)
				// if the value passed in is the zero value of the appropriate type
				if value.Kind() != reflect.String || val == "" {
					return errors.New("value is required")
				}
				// make sure we can make this into an absolute directory
				_, err := filepath.Abs(val.(string))
				return err
			},
		},
	}
	err = survey.Ask(qs, &name)
	if err != nil {
		return
	}
	path, err = filepath.Abs(name)
	if err != nil {
		return
	}
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return
	}
	if exists {
		dir, err := afero.IsDir(fs, path)
		if err != nil {
			return "", "", fmt.Errorf("unable to get determine if %q is a directory: %v", path, err)
		}
		e, err := afero.IsEmpty(fs, path)
		if err != nil {
			return "", "", fmt.Errorf("unable to get determine if %q is empty: %v", path, err)
		}
		if !dir || !e {
			var nuke bool
			err = survey.AskOne(&survey.Confirm{
				Message: fmt.Sprintf("target directory %q is not empty. Remove existing files and continue?", path),
				Default: false,
			}, &nuke)
			if err != nil {
				return "", "", fmt.Errorf("unable to get determine if %q should be removed: %v", path, err)
			}
			if !nuke {
				return "", "", errors.New("Initialize cancelled")
			}
			err = fs.RemoveAll(path)
			if err != nil {
				return "", "", fmt.Errorf("unable to remove directory %q: %v", path, err)
			}
		}
	}
	err = fs.MkdirAll(path, os.ModeDir|os.ModePerm)
	return name, path, err
}

type genFile struct {
	name       string
	content    string
	permission os.FileMode
}

func generateManifest(p transformProject) (map[string][]genFile, error) {
	switch p.Lang {
	case WasmLangGo:
		rpConfig, err := marshalConfig(WasmProjectConfig{Name: p.Name, Topic: p.Topic, Language: p.Lang})
		if err != nil {
			return nil, err
		}
		goMod, err := template.WasmGoModule(p.Name)
		if err != nil {
			return nil, err
		}
		return map[string][]genFile{
			p.Path: {
				genFile{name: "transform.go", content: template.WasmGoMain()},
				genFile{name: "redpandarc.yaml", content: string(rpConfig)},
				genFile{name: "go.mod", content: goMod},
				genFile{name: "go.sum", content: template.WasmGoChecksums()},
				genFile{name: "README.md", content: template.WasmGoReadme()},
			},
		}, nil
	}
	return nil, errors.New("unknown wasm language")
}

func executeGenerate(fs afero.Fs, p transformProject) error {
	fmt.Printf("generating project in %s...\n", p.Path)
	manifest, err := generateManifest(p)
	if err != nil {
		return err
	}
	for dir, templates := range manifest {
		if err := fs.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		for _, template := range templates {
			file := filepath.Join(dir, template.name)
			perm := os.FileMode(0o600)
			if template.permission > 0 {
				perm = template.permission
			}
			if err := afero.WriteFile(fs, file, []byte(template.content), perm); err != nil {
				return err
			}
		}
	}
	fmt.Printf("created project in %s. now run:\n\n", p.Path)
	fmt.Println("  cd", p.Name)
	fmt.Println("  rpk wasm build")
	fmt.Println("  rpk wasm deploy")
	return nil
}
