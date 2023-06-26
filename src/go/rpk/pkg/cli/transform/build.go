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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/buildpack"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newBuildCommand(fs afero.Fs, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a transform",
		Long: `Build a transform.

This command looks in the current working directory for a transform.yaml file.

Then depending on the language, will install the appropriate build plugin then 
build a .wasm file.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := project.LoadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", project.ConfigFileName)
			switch cfg.Language {
			case project.WasmLangTinygo:
				tinygo, err := installBuildpack(cmd.Context(), buildpack.Tinygo, fs)
				out.MaybeDie(err, "unable to install tinygo plugin: %v", err)
				args := []string{"-o", fmt.Sprintf("%s.wasm", cfg.Name)}
				out.MaybeDieErr(execFn(tinygo, args))
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
		},
	}
	return cmd
}

func installBuildpack(ctx context.Context, p buildpack.Buildpack, fs afero.Fs) (path string, err error) {
	ok, err := p.IsUpToDate(fs)
	if err != nil {
		return "", err
	}
	if ok {
		return p.BinPath()
	}
	fmt.Printf("latest %s buildpack not found, downloading now...\n", p.Name)
	err = p.Download(ctx, fs)
	if err != nil {
		return "", err
	}
	fmt.Printf("latest %s buildpack download complete\n", p.Name)
	return p.BinPath()
}
