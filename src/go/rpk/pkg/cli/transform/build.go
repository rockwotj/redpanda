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
	"path/filepath"
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
	Name string
	// the base url to download the plugin from, the end should be `/{name}-{goos}-{goarch}.tar.gz`
	baseUrl string
	// [GOOS][GOARCH] = shasum
	shaSums map[string]map[string]string
	// A callback to modify arguments before executing the command
	modifyArgs func(c *cobra.Command, p WasmProjectConfig, args []string) []string
	// Other hidden plugins that are required by this plugin
	deps []buildPlugin
}

func (bp *buildPlugin) ModifyArgs(c *cobra.Command, p WasmProjectConfig, args []string) []string {
	if bp.modifyArgs != nil {
		return bp.modifyArgs(c, p, args)
	}
	return args
}

func (bp *buildPlugin) Deps() []buildPlugin {
	if bp.deps == nil {
		return []buildPlugin{}
	}
	return bp.deps
}

func (bp *buildPlugin) PlatformSha() (sha string, err error) {
	a, ok := bp.shaSums[runtime.GOOS]
	if ok {
		archSha, ok := a[runtime.GOARCH]
		if ok {
			sha = archSha
		}
	}
	if sha == "" {
		err = fmt.Errorf("%s plugin is not supported in this environment", bp.Name)
	}
	return
}

func (bp *buildPlugin) Url() string {
	return fmt.Sprintf(
		"%s/%s-%s-%s.tar.gz",
		bp.baseUrl,
		bp.Name,
		runtime.GOOS,
		runtime.GOARCH,
	)
}

var (
	tinygoPlugin = buildPlugin{
		Name:    "tinygo",
		baseUrl: "https://github.com/rockwotj/tinygo/releases/download/v0.28.1-rpk",
		shaSums: map[string]map[string]string{
			"linux": map[string]string{
				"amd64": "f2dff481f71319007f7715d61cabf60643838dd7ceeda05d7af68811d13e813d",
				"arm64": "e42e393e073b4955eb3b25f630dfd0d013328e8d7664bc9efe97c76b833cf811",
			},
			"darwin": map[string]string{
				"amd64": "923130c6d524d98a9521336acb486d596a1ee28861c9c28a91fdcc9902349425",
				"arm64": "11714ef300a4f236b211753d4c58bf60c3400a163f599af023b1ea52835f12fe",
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
		deps: []buildPlugin{
			{
				Name:    "wasm-opt",
				baseUrl: "https://github.com/rockwotj/tinygo/releases/download/v0.28.1-rpk",
				shaSums: map[string]map[string]string{
					"linux": map[string]string{
						"amd64": "e43bc07ec16e4d1758aaeaaf5b2bbe820760d87fdcdb9fbcd235818aa4add107",
						"arm64": "5a1642783a57d233b5050ace12a57801652878d8f9b8c7d9fc1be4cf84c649c1",
					},
					"darwin": map[string]string{
						"amd64": "a233945f05c85edbafa4ce39db002f284aad9e143278b8626aa1dbc4a0fcfe97",
						"arm64": "daa18a3e6fec1f6d5b45c3e66d4e153ca7948212a5bf53e0b565bc7341102734",
					},
				},
			},
		},
	}
)

func init() {
	for _, p := range []buildPlugin{tinygoPlugin} {
		plugin.RegisterManaged(p.Name, []string{"transform", "build", p.Name}, func(c *cobra.Command, fs afero.Fs, _ *config.Params) *cobra.Command {
			run := c.Run
			c.Run = func(cmd *cobra.Command, args []string) {
				cfg, err := loadCfg(fs)
				out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
				_, err = installPlugin(cmd.Context(), tinygoPlugin, fs)
				out.MaybeDie(err, "unable to install %s plugin: %v", p.Name, err)
				args = p.ModifyArgs(cmd, cfg, args)
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
		Long: `Build a transform.

This command looks in the current working directory for a transform.yaml file.

Then depending on the language, will install the appropriate build plugin then 
build a .wasm file.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := loadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
			switch cfg.Language {
			case WasmLangTinygo:
				tinygo, err := installPlugin(cmd.Context(), tinygoPlugin, fs)
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

func installPlugin(ctx context.Context, p buildPlugin, fs afero.Fs) (path string, err error) {
	sha, err := p.PlatformSha()
	if err != nil {
		return "", err
	}
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", fmt.Errorf("unable to determine managed plugin path: %v", err)
	}
	tinygo, pluginExists := plugin.ListPlugins(fs, []string{pluginDir}).Find(p.Name)
	if pluginExists {
		fsha, err := plugin.Sha256Path(fs, tinygo.Path)
		if err != nil {
			return "", fmt.Errorf("unable to determine if plugin is up to date: %v", err)
		}
		// TODO: We should verify that deps are present and the right shas
		if sha == fsha {
			return tinygo.Path, nil
		}
	}
	fmt.Printf("latest %s build plugin not found, downloading now...\n", p.Name)
	path, err = downloadPlugin(ctx, p, fs, sha, false)
	if err != nil {
		return "", err
	}
	for _, dep := range p.Deps() {
		depSha, err := dep.PlatformSha()
		if err != nil {
			return "", err
		}
		_, err = downloadPlugin(ctx, dep, fs, depSha, true)
		if err != nil {
			return "", err
		}
	}
	fmt.Printf("latest %s build plugin download complete\n", p.Name)
	return path, nil
}

func downloadPlugin(ctx context.Context, p buildPlugin, fs afero.Fs, expectedSha string, dep bool) (path string, err error) {
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", fmt.Errorf("unable to determine managed plugin path: %v", err)
	}
	bin, err := plugin.Download(ctx, p.Url(), true, expectedSha)
	if exists, _ := afero.DirExists(fs, pluginDir); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", pluginDir)
		}
		err = os.MkdirAll(pluginDir, 0o755)
		if err != nil {
			return "", fmt.Errorf("unable to create plugin bin directory: %v", err)
		}
	}
	if dep {
		path = filepath.Join(pluginDir, p.Name)
		err = rpkos.ReplaceFile(fs, path, bin, 0o755)
	} else {
		path, err = plugin.WriteBinary(fs, p.Name, pluginDir, bin, false, true)
	}
	if err != nil {
		return "", fmt.Errorf("unable to write %s plugin to disk: %v", p.Name, err)
	}
	return path, nil
}
