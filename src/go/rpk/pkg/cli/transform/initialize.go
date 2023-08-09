// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package transform

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/buildpack"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/template"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type transformProject struct {
	Name string
	Path string
	Lang project.WasmLang
}

func newInitializeCommand(fs afero.Fs) *cobra.Command {
	var (
		langVal string
		name    string
	)
	cmd := &cobra.Command{
		Use:   "init [DIRECTORY]",
		Short: "Initialize a transform",
		Long: `Initialize a transform.

Creates a new transform using a template in the current directory.

A new directory can be created by specifying it in the command, like:

rpk transform init foobar

Will initialize a transform project in the foobar directory.
		`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var path string
			cwd, err := os.Getwd()
			out.MaybeDie(err, "unable to get current directory: %v", err)
			cwd, err = filepath.Abs(cwd)
			out.MaybeDie(err, "unable to determine path for %q: %v", cwd, err)
			if len(args) == 0 {
				path = cwd
			} else {
				path = args[0]
				ok, err := afero.Exists(fs, path)
				out.MaybeDie(err, "unable to determine if %q exists: %v", path, err)
				if !ok {
					err = fs.MkdirAll(path, os.ModeDir|os.ModePerm)
					out.MaybeDie(err, "unable to create directory %q: %v", path, err)
				}
				path, err = filepath.Abs(path)
				out.MaybeDie(err, "unable to determine an absolute path for %q: %v", path, err)
				f, err := fs.Stat(path)
				out.MaybeDie(err, "unable to determine if %q exists: %v", path, err)
				if !f.IsDir() {
					out.Die("please remove file %q to initialize a transform there", path)
				}
			}
			c := filepath.Join(path, project.ConfigFileName)
			ok, err := afero.Exists(fs, c)
			out.MaybeDie(err, "unable to determine if %q exists: %v", c, err)
			if ok {
				out.Die("there is already a transform at %q, please delete it before retrying", c)
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
			var lang project.WasmLang
			if langVal == "" {
				langVal, err := out.Pick(project.AllWasmLangs, "select a language:")
				out.MaybeDie(err, "unable to determine transform language: %v", err)
				lang = project.WasmLang(langVal)
			} else {
				needle := strings.ToLower(langVal)
				for _, v := range project.AllWasmLangs {
					if strings.ToLower(v) == needle {
						lang = project.WasmLang(v)
						break
					}
				}
				if lang == "" {
					out.Die("unknown language %q", langVal)
				}
			}
			p := transformProject{Name: name, Path: path, Lang: lang}
			err = executeGenerate(fs, p)
			out.MaybeDie(err, "unable to generate all manifest files: %v", err)
			ok, err = out.Confirm("install dependencies?")
			if ok && err == nil {
				installDeps(cmd.Context(), fs, p)
			}
			fmt.Println("deploy your transform using:")
			if cwd != path {
				rel, err := filepath.Rel(cwd, path)
				if err == nil {
					fmt.Println("\tcd", rel)
				}
			}
			fmt.Println("\trpk transform build")
			fmt.Println("\trpk transform deploy")
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
	if p.Lang == project.WasmLangTinygo {
		rpConfig, err := project.MarshalConfig(project.Config{Name: p.Name, Language: p.Lang})
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
				genFile{name: project.ConfigFileName, content: string(rpConfig)},
				genFile{name: "go.mod", content: goMod},
				genFile{name: "README.md", content: template.WasmGoReadme()},
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown language %q", p.Lang)
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
			perm := os.FileMode(0o644)
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

func installDeps(ctx context.Context, fs afero.Fs, p transformProject) error {
	if p.Lang == project.WasmLangTinygo {
		_, err := installBuildpack(ctx, buildpack.Tinygo, fs)
		out.MaybeDie(err, "unable to install tinygo buildpack: %v", err)
		g, err := exec.LookPath("go")
		out.MaybeDie(err, "go is not available on $PATH, please download and install it: https://go.dev/doc/install")
		runGoCli := func(args ...string) {
			c := exec.CommandContext(ctx, g, args...)
			c.Stderr = os.Stderr
			c.Stdin = os.Stdin
			c.Stdout = os.Stdout
			c.Dir = p.Path
			out.MaybeDieErr(c.Run())
		}
		runGoCli("get", "github.com/redpanda-data/redpanda/src/go/sdk@transform-dev")
		runGoCli("mod", "tidy")
		fmt.Println("go modules are tidy 🧹")
	}
	return fmt.Errorf("Unknown language %q", p.Lang)
}
