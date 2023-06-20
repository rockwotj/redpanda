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
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

var tinygoPluginSha256 string

func init() {
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		tinygoPluginSha256 = "6cc3ffbbba25429b46bb83dc36a87e4d83e5b33d9e562492339c91e4338ca95b"
	} else if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
		tinygoPluginSha256 = "ec86763291dd3993f83db4600f529451d8f31d70a85917573536b759808607b3"
	}
	plugin.RegisterManaged("tinygo", []string{"transform", "build", "tinygo"}, func(c *cobra.Command, fs afero.Fs, _ *config.Params) *cobra.Command {
		run := c.Run
		c.Run = func(cmd *cobra.Command, args []string) {
			cfg, err := loadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
			_, err = installPlugin(cmd.Context(), fs)
			out.MaybeDie(err, "unable to install tinygo plugin", err)
			for _, arg := range args {
				if arg == "-o" || strings.HasPrefix(arg, "-o=") {
					run(cmd, args)
					return
				}
			}
			run(cmd, append(args, "-o", fmt.Sprintf("%s.wasm", cfg.Name)))
		}
		return c
	})
}

func newBuildCommand(fs afero.Fs, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a transform",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := loadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
			switch cfg.Language {
			case WasmLangTinygo:
				tinygo, err := installPlugin(cmd.Context(), fs)
				out.MaybeDie(err, "unable to install tinygo plugin: %v", err)
				out.MaybeDieErr(execFn(tinygo, []string{"-o", fmt.Sprintf("%s.wasm", cfg.Name)}))
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
		},
	}
	return cmd
}

func installPlugin(ctx context.Context, fs afero.Fs) (path string, err error) {
	if tinygoPluginSha256 == "" {
		return "", errors.New("tinygo plugin is not supported in this environment")
	}
	// First load our configuration and token.
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", fmt.Errorf("unable to determine managed plugin path: %v", err)
	}
	tinygo, pluginExists := plugin.ListPlugins(fs, []string{pluginDir}).Find("tinygo")
	if pluginExists {
		sha, err := plugin.Sha256Path(fs, tinygo.Path)
		if err != nil {
			return "", fmt.Errorf("unable to determine if plugin is up to date: %v", err)
		}
		if sha == tinygoPluginSha256 {
			return tinygo.Path, nil
		}
	}
	url := fmt.Sprintf(
		"https://github.com/rockwotj/tinygo/releases/download/v.0.28.1/tinygo-%s-%s.tar.gz",
		runtime.GOOS,
		runtime.GOARCH,
	)
	fmt.Println("latest tinygo build plugin not found, downloading now...")
	bin, err := plugin.Download(ctx, url, true, tinygoPluginSha256)
	if exists, _ := afero.DirExists(fs, pluginDir); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", pluginDir)
		}
		err = os.MkdirAll(pluginDir, 0o755)
		if err != nil {
			return "", fmt.Errorf("unable to create plugin bin directory: %v", err)
		}
	}
	path, err = plugin.WriteBinary(fs, "tinygo", pluginDir, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write tinygo plugin to disk: %v", err)
	}
	fmt.Println("latest tinygo build plugin download complete ✔️")
	return path, nil
}
