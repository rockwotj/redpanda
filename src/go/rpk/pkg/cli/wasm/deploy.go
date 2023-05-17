package wasm

import (
	"bytes"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy Wasm function",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			wasmCfg, err := loadCfg(fs)
			out.MaybeDie(err, "Unable to find redpandarc.yaml in the currect directory")

			topic := wasmCfg.Topic
			path := fmt.Sprintf("%s.wasm", wasmCfg.Name)

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "missing %q: %v did you run `rpk wasm build`", path, err)

			err = api.DeployWasm(cmd.Context(), topic, bytes.NewReader(file))
			out.MaybeDie(err, "unable to deploy wasm %q: %v", path, err)

			fmt.Println("Deploy successful!")
		},
	}
	return cmd
}
