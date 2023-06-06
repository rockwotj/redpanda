package wasm

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUndeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "undeploy [NAME]",
		Short: "Undeploy a Wasm transform",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)
			functionName := ""
			if len(args) == 1 {
				functionName = args[0]
			}
			if functionName == "" {
				cfg, err := loadCfg(fs)
				if err == nil {
					functionName = cfg.Name
				}
			}

			transforms, err := api.ListWasmTransforms(cmd.Context())
			out.MaybeDie(err, "unable to list existing transforms: %v", err)

			var selected *admin.ClusterWasmTransform = nil
			for _, t := range transforms {
				if t.FunctionName == functionName {
					selected = &t
					break
				}
			}
			if selected == nil {
				out.Die("unknown transform %q", functionName)
			}

			err = api.UndeployWasmTransform(cmd.Context(), *selected)
			out.MaybeDie(err, "unable to undeploy transform %q: %v", functionName, err)

			fmt.Println("Undeploy successful!")
		},
	}
	return cmd
}
