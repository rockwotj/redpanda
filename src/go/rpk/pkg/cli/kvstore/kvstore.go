// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kvstore

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kvstore",
		Short: "Interact with topic key-value stores",
		Long:  "Commands to interact with key-value stores associated with Redpanda topics.",
	}
	p.InstallKafkaFlags(cmd)
	cmd.AddCommand(
		newGetCommand(fs, p),
		newPutCommand(fs, p),
		newDeleteCommand(fs, p),
		newScanCommand(fs, p),
	)
	return cmd
}
