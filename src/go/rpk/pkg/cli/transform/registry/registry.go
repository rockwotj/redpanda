// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/go-github/v53/github"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
)

type (
	Artifact struct {
		Name        string
		DownloadUrl string
	}
	Transform struct {
		ID          string
		Name        string
		Description string
		Version     string
		Artifacts   []Artifact
	}
)

const (
	RedpandaRegistryName      = "redpanda-data"
	RedpandaRegistryRepoOwner = "rockwotj"
	RedpandaRegistryRepoName  = "redpanda-data-transforms"
	semverPattern             = `^v?([0-9]+)(\.[0-9]+)(\.[0-9]+)$`
)

var versionRegex *regexp.Regexp

func init() {
	versionRegex = regexp.MustCompile(semverPattern)
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "View configurable prebuilt data transforms",
	}
	cmd.AddCommand(
		newListCommand(fs, p),
		newViewCommand(fs, p),
	)
	return cmd
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all the modules in the registry",
		Run: func(cmd *cobra.Command, args []string) {
			ts, err := ListTransforms(cmd.Context(), RedpandaRegistryName)
			out.MaybeDie(err, "unable to list transforms: %v", err)
			for _, t := range ts {
				out.Section(t.Name + " " + t.Version)
				fmt.Println(t.Description)
			}
		},
	}
	return cmd
}

func newViewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe [MODULE]",
		Short: "Describe a registry module",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			r, n, v, err := ParseRegistryString(args[0])
			out.MaybeDieErr(err)
			t, err := GetTransform(cmd.Context(), r, n, v)
			out.MaybeDie(err, "unable to get transform: %v", err)
			out.Section(t.Name + " " + t.Version)
			fmt.Println(t.Description)
		},
	}
	return cmd
}

func dedupe(transforms []Transform) (latest []Transform) {
	seen := map[string]bool{}
	for _, t := range transforms {
		_, ok := seen[t.ID]
		if ok {
			continue
		}
		seen[t.ID] = true
		latest = append(latest, t)
	}
	return
}

func ListTransforms(ctx context.Context, registry string) (transforms []Transform, err error) {

	if registry != RedpandaRegistryName {
		return nil, fmt.Errorf("unknown registry %q", registry)
	}
	var tc *http.Client = nil
	token, ok := os.LookupEnv("GITHUB_TOKEN")
	if ok {
		ts := oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: token,
		})
		tc = oauth2.NewClient(ctx, ts)
	}
	client := github.NewClient(tc)
	for page := 1; ; page++ {
		opts := &github.ListOptions{
			Page:    page,
			PerPage: 100,
		}
		releases, _, err := client.Repositories.ListReleases(ctx, RedpandaRegistryRepoOwner, RedpandaRegistryRepoName, opts)
		if err != nil {
			return nil, err
		}
		for _, r := range releases {
			tag := r.GetTagName()
			ts := strings.LastIndexByte(tag, '-')
			fullName := r.GetName()
			ns := strings.LastIndexByte(fullName, ' ')
			// TODO: Handle malformed names and tags
			t := Transform{
				ID:          tag[:ts],
				Name:        fullName[:ns],
				Version:     tag[ns+1:],
				Description: r.GetBody(),
				Artifacts:   make([]Artifact, 0),
			}
			for _, a := range r.Assets {
				t.Artifacts = append(t.Artifacts, Artifact{
					Name:        a.GetName(),
					DownloadUrl: a.GetBrowserDownloadURL(),
				})
			}
			transforms = append(transforms, t)
		}
		if len(releases) < opts.PerPage {
			break
		}
	}
	transforms = dedupe(transforms)
	return
}

func GetTransform(ctx context.Context, registry, id, version string) (t Transform, err error) {
	all, err := ListTransforms(ctx, registry)
	if err != nil {
		return t, err
	}
	for _, t := range all {
		if version != "" && t.Version != version {
			continue
		}
		if t.ID == id {
			return t, nil
		}
	}
	return t, errors.New("transform not found")
}

func DownloadTransform(ctx context.Context, registry, id, version string) (cfg project.Config, wasm io.Reader, err error) {
	var (
		cfgUrl  string
		wasmUrl string
		t       Transform
	)
	t, err = GetTransform(ctx, registry, id, version)
	if err != nil {
		return cfg, wasm, err
	}
	for _, a := range t.Artifacts {
		if filepath.Ext(a.Name) == ".wasm" && wasmUrl == "" {
			wasmUrl = a.DownloadUrl
		} else if a.Name == project.ConfigFileName && cfgUrl == "" {
			cfgUrl = a.DownloadUrl
		} else {
			return cfg, wasm, fmt.Errorf("unknown asset: %s", a.Name)
		}
	}
	if wasmUrl == "" || cfgUrl == "" {
		return cfg, wasm, fmt.Errorf("missing assets")
	}
	cl := httpapi.NewClient()
	var rawCfg []byte
	err = cl.Get(ctx, cfgUrl, nil, &rawCfg)
	if err != nil {
		return cfg, wasm, err
	}
	cfg, err = project.UnmarshalConfig(rawCfg)
	if err != nil {
		return cfg, wasm, err
	}
	// Remove the name from the config, the user must provide one
	cfg.Name = ""
	err = cl.Get(ctx, wasmUrl, nil, &wasm)
	return cfg, wasm, err
}

func ParseRegistryString(s string) (registry string, id string, version string, err error) {
	if !strings.HasPrefix(s, "@") {
		err = fmt.Errorf("%q is not a registry identifier", s)
		return
	}
	s = strings.TrimPrefix(s, "@")
	p := strings.SplitN(s, "/", 2)
	if len(p) != 2 {
		err = fmt.Errorf("%q is missing a module identifer", s)
		return
	}
	registry = p[0]
	id = p[1]
	p = strings.SplitN(id, "@", 2)
	if len(p) == 1 {
		return
	}
	id = p[0]
	version = p[1]
	if !versionRegex.MatchString(version) {
		err = fmt.Errorf("%q is not a valid version", version)
	}
	return
}
