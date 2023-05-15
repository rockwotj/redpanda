package wasm

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deploy [TOPIC] [PATH]",
		Short: "Deploy Wasm function",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			topic := args[0]
			path := args[1]
			if filepath.Ext(path) != ".wasm" {
				out.Die("cannot deploy %q: only .wasm files are supported", path)
			}

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "unable to read %q: %v", path, err)

			err = api.DeployWasm(cmd.Context(), topic, bytes.NewReader(file))
			out.MaybeDie(err, "unable to deploy wasm %q: %v", path, err)

			fmt.Println("Deploy successful!")
		},
	}
	return cmd
}
