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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func getTransformByName(transforms []adminapi.ClusterWasmTransform, name string) *adminapi.ClusterWasmTransform {
	for _, t := range transforms {
		if t.Name == name {
			return &t
		}
	}
	return nil
}

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete [NAME]",
		Short: "Delete a data transform",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)
			functionName := args[0]
			transforms, err := api.ListWasmTransforms(cmd.Context())
			out.MaybeDie(err, "unable to list existing transforms: %v", err)

			selected := getTransformByName(transforms, functionName)
			if selected == nil {
				out.Die("unknown transform %q", functionName)
			}

			err = api.DeleteWasmTransform(cmd.Context(), *selected)
			out.MaybeDie(err, "unable to delete transform %q: %v", functionName, err)

			fmt.Println("Delete successful!")
		},
	}
	return cmd
}
