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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/registry"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "transform",
		Aliases:    []string{"wasm"},
		SuggestFor: []string{"transfrom"},
		Short:      "Develop, deploy and manage Redpanda data transforms",
	}
	p.InstallKafkaFlags(cmd)
	cmd.AddCommand(
		newInitializeCommand(fs, p),
		newBuildCommand(fs, execFn),
		newDeployCommand(fs, p),
		newListCommand(fs, p),
		newDeleteCommand(fs, p),
		registry.NewCommand(fs, p),
	)
	return cmd
}
