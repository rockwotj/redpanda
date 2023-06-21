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
	"runtime"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type buildPlugin struct {
	// the name of the plugin, this will also be the command name.
	name string
	// the base url to download the plugin from, the end should be `/{name}-{goos}-{goarch}.tar.gz`
	baseUrl string
	// [GOOS][GOARCH] = shasum
	shaSums map[string]map[string]string
	// A callback to modify arguments before executing the command
	modifyArgs func(c *cobra.Command, p WasmProjectConfig, args []string) []string
}

var (
	tinygoPlugin = buildPlugin{
		name:    "tinygo",
		baseUrl: "https://github.com/rockwotj/tinygo/releases/download/v0.28.1-rpk",
		shaSums: map[string]map[string]string{
			"linux": map[string]string{
				"amd64": "f2dff481f71319007f7715d61cabf60643838dd7ceeda05d7af68811d13e813d",
				"arm64": "e42e393e073b4955eb3b25f630dfd0d013328e8d7664bc9efe97c76b833cf811",
			},
			"darwin": map[string]string{
				"amd64": "",
				"arm64": "",
			},
		},
		modifyArgs: func(c *cobra.Command, p WasmProjectConfig, args []string) []string {
			// Add the output flag if not specified
			for _, arg := range args {
				if arg == "-o" || strings.HasPrefix(arg, "-o=") {
					return args
				}
			}
			return append(args, "-o", fmt.Sprintf("%s.wasm", p.Name))
		},
	}
)

func init() {
	for _, p := range []buildPlugin{tinygoPlugin} {
		plugin.RegisterManaged(p.name, []string{"transform", "build", p.name}, func(c *cobra.Command, fs afero.Fs, _ *config.Params) *cobra.Command {
			run := c.Run
			c.Run = func(cmd *cobra.Command, args []string) {
				cfg, err := loadCfg(fs)
				out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
				_, err = installPlugin(cmd.Context(), tinygoPlugin, fs)
				out.MaybeDie(err, "unable to install %s plugin: %v", p.name, err)
				if p.modifyArgs != nil {
					args = p.modifyArgs(cmd, cfg, args)
				}
				run(cmd, args)
			}
			return c
		})
	}
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
				tinygo, err := installPlugin(cmd.Context(), tinygoPlugin, fs)
				out.MaybeDie(err, "unable to install tinygo plugin: %v", err)
				out.MaybeDieErr(execFn(tinygo, []string{"-o", fmt.Sprintf("%s.wasm", cfg.Name)}))
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
		},
	}
	return cmd
}

func installPlugin(ctx context.Context, p buildPlugin, fs afero.Fs) (path string, err error) {
	sha := ""
	a, ok := p.shaSums[runtime.GOOS]
	if ok {
		archSha, ok := a[runtime.GOARCH]
		if ok {
			sha = archSha
		}
	}
	if sha == "" {
		return "", fmt.Errorf("%s plugin is not supported in this environment", p.name)
	}
	// First load our configuration and token.
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", fmt.Errorf("unable to determine managed plugin path: %v", err)
	}
	tinygo, pluginExists := plugin.ListPlugins(fs, []string{pluginDir}).Find(p.name)
	if pluginExists {
		fsha, err := plugin.Sha256Path(fs, tinygo.Path)
		if err != nil {
			return "", fmt.Errorf("unable to determine if plugin is up to date: %v", err)
		}
		if sha == fsha {
			return tinygo.Path, nil
		}
	}
	url := fmt.Sprintf(
		"%s/%s-%s-%s.tar.gz",
		p.baseUrl,
		p.name,
		runtime.GOOS,
		runtime.GOARCH,
	)
	fmt.Println(url)
	fmt.Printf("latest %s build plugin not found, downloading now...\n", p.name)
	bin, err := plugin.Download(ctx, url, true, sha)
	if exists, _ := afero.DirExists(fs, pluginDir); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", pluginDir)
		}
		err = os.MkdirAll(pluginDir, 0o755)
		if err != nil {
			return "", fmt.Errorf("unable to create plugin bin directory: %v", err)
		}
	}
	path, err = plugin.WriteBinary(fs, p.name, pluginDir, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write %s plugin to disk: %v", p.name, err)
	}
	fmt.Printf("latest %s build plugin download complete\n", p.name)
	return path, nil
}
