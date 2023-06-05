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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/wasm/template"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type transformProject struct {
	Name string
	Path string
	Lang WasmLang
}

func newInitializeCommand(fs afero.Fs, cfg *config.Params) *cobra.Command {
	var (
		langVal string
		name    string
	)
	cmd := &cobra.Command{
		Use:   "init [DIRECTORY]",
		Short: "Initialize a Wasm transform",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var path string
			if len(args) == 0 {
				cwd, err := os.Getwd()
				out.MaybeDie(err, "unable to get current directory: %v", err)
				path = cwd
			} else {
				path = args[0]
				ok, err := afero.Exists(fs, path)
				out.MaybeDie(err, "unable to determine if %q exists: %v", path, err)
				if !ok {
					err = fs.MkdirAll(path, os.ModeDir|os.ModePerm)
					out.MaybeDie(err, "unable to create directory %q: %v", path, err)
				}
				f, err := fs.Stat(path)
				out.MaybeDie(err, "unable to determine if %q exists: %v", path, err)
				if !f.IsDir() {
					out.Die("please remove file %q to initialize a transform there", path)
				}
			}
			for name == "" {
				suggestion := filepath.Base(path)
				if suggestion == "." {
					suggestion = ""
				}
				var err error
				name, err = out.PromptWithSuggestion(suggestion, "name this transform:")
				out.MaybeDie(err, "unable to determine project name: %v", err)
			}
			c := filepath.Join(path, configFileName)
			ok, err := afero.Exists(fs, c)
			out.MaybeDie(err, "unable to determine if %q exists: %v", c, err)
			if ok {
				out.Die("there is already a transform at %q", c)
			}
			var lang WasmLang
			if langVal == "" {
				langVal, err := out.Pick(AllWasmLangs, "select a language:")
				out.MaybeDie(err, "unable to determine transform language: %v", err)
				lang = WasmLang(langVal)
			} else {
				needle := strings.ToLower(langVal)
				for _, v := range AllWasmLangs {
					if strings.ToLower(v) == needle {
						lang = WasmLang(v)
						break
					}
				}
				if lang == "" {
					out.Die("unknown wasm language %q", langVal)
				}
			}
			path, err = filepath.Abs(path)
			out.MaybeDie(err, "unable to determine path for %q: %v", path, err)
			p := transformProject{Name: name, Path: path, Lang: lang}
			err = executeGenerate(fs, p)
			out.MaybeDie(err, "unable to generate all manifest files: %v", err)
			ok, err = out.Confirm("install dependencies?")
			if ok && err == nil {
				installDeps(cmd.Context(), p)
			}
			fmt.Println("deploy your transform using:")
			fmt.Println("\trpk wasm build")
			fmt.Println("\trpk wasm deploy")
		},
	}
	cmd.Flags().StringVar(&langVal, "lang", "", "The language used to develop the transform")
	cmd.Flags().StringVar(&name, "name", "", "The name of the transform")
	return cmd
}

type genFile struct {
	name       string
	content    string
	permission os.FileMode
}

func generateManifest(p transformProject) (map[string][]genFile, error) {
	switch p.Lang {
	case WasmLangTinygo:
		rpConfig, err := marshalConfig(WasmProjectConfig{Name: p.Name, Language: p.Lang})
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
				genFile{name: configFileName, content: string(rpConfig)},
				genFile{name: "go.mod", content: goMod},
				genFile{name: "README.md", content: template.WasmGoReadme()},
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown wasm language %q", p.Lang)
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
	fmt.Println("created project in", p.Path, "🚀")
	return nil
}

func installDeps(ctx context.Context, p transformProject) error {
	switch p.Lang {
	case WasmLangTinygo:
		{
			g, err := exec.LookPath("go")
			out.MaybeDie(err, "go is not available on $PATH, please download and install it: https://go.dev/doc/install")
			c := exec.CommandContext(ctx, g, "mod", "tidy")
			c.Dir = p.Path
			out.MaybeDieErr(c.Run())
			fmt.Println("go modules are tidy 🧹")
		}
	}
	return fmt.Errorf("Unknown wasm language %q", p.Lang)
}
