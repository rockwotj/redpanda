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
	"fmt"
	"os"
	"path/filepath"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/wasm/template"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newInitializeCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init [PROJECT DIRECTORY]",
		Short: "Create a template project for Wasm engine",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			path, err := filepath.Abs(args[0])
			out.MaybeDie(err, "unable to get absolute path for %q: %v", args[0], err)
			err = executeGenerate(fs, path)
			out.MaybeDie(err, "unable to generate all manifest files: %v", err)
		},
	}
	return cmd
}

type genFile struct {
	name       string
	content    string
	permission os.FileMode
}

func generateManifest(version string) map[string][]genFile {
	return map[string][]genFile{
		".": {
			genFile{name: "transform.go", content: template.WasmGoMain()},
			genFile{name: "go.mod", content: template.WasmGoModule()},
			genFile{name: "README.md", content: template.WasmGoReadme()},
		},
	}
}

const defAPIVersion = "21.8.2"

func executeGenerate(fs afero.Fs, path string) error {
	var preexisting []string
	var version string
	for dir, templates := range generateManifest(version) {
		for _, template := range templates {
			file := filepath.Join(path, dir, template.name)
			exist, err := afero.Exists(fs, file)
			if err != nil {
				return fmt.Errorf("unable to determine if file %q exists: %v", file, err)
			}
			if exist {
				preexisting = append(preexisting, file)
			}
		}
	}
	if len(preexisting) > 0 {
		return fmt.Errorf("files already exist; try using a new directory or removing the existing files, existing: %v", preexisting)
	}

	if err := fs.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for dir, templates := range generateManifest(version) {
		dirPath := filepath.Join(path, dir)
		if err := fs.MkdirAll(dirPath, 0o755); err != nil {
			return err
		}
		for _, template := range templates {
			file := filepath.Join(dirPath, template.name)
			perm := os.FileMode(0o600)
			if template.permission > 0 {
				perm = template.permission
			}
			if err := afero.WriteFile(fs, file, []byte(template.content), perm); err != nil {
				return err
			}
		}
	}
	fmt.Printf("created project in %s\n", path)
	return nil
}
