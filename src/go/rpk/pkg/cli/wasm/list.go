package wasm

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Wasm functions",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			l, err := api.ListWasmTransforms(cmd.Context())
			out.MaybeDie(err, "unable to list wasm functions: %v", err)

			w := out.NewTable("NAME", "INPUT TOPIC", "OUTPUT TOPIC")
			defer w.Flush()
			for _, t := range l {
				w.PrintStrings(t.FunctionName, t.InputTopic, t.OutputTopic)
			}
		},
	}
	return cmd
}
