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
	var (
		inputTopic   string
		outputTopic  string
		functionName string
	)
	cmd := &cobra.Command{
		Use:   "deploy [WASM]",
		Short: "Deploy a Wasm transform",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if functionName == "" {
				cfg, err := loadCfg(fs)
				out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
				functionName = cfg.Name
			}
			var path string
			if len(args) == 1 {
				path = args[0]
			} else {
				path = fmt.Sprintf("%s.wasm", functionName)
			}
			ok, err := afero.Exists(fs, path)
			out.MaybeDie(err, "missing %q: %v", path, err)
			if !ok {
				out.Die("missing %q", path)
			}
			if inputTopic == "" {
				cfg, err := loadCfg(fs)
				if err == nil && cfg.InputTopic != "" {
					inputTopic = cfg.InputTopic
				} else {
					inputTopic, err = out.Prompt("Select an input topic:")
					out.MaybeDie(err, "no input topic: %v", err)
				}
			}
			if outputTopic == "" {
				outputTopic, err = out.Prompt("Select an output topic:")
				out.MaybeDie(err, "no input topic: %v", err)
			}

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "missing %q: %v did you run `rpk wasm build`", path, err)

			err = api.DeployWasmTransform(cmd.Context(), inputTopic, outputTopic, functionName, bytes.NewReader(file))
			out.MaybeDie(err, "unable to deploy wasm %q: %v", path, err)

			fmt.Println("Deploy successful!")
		},
	}
	cmd.Flags().StringVar(&inputTopic, "input-topic", "", "The input topic to apply the transform to")
	cmd.Flags().StringVar(&outputTopic, "output-topic", "", "The output topic to write the transform results to")
	cmd.Flags().StringVar(&functionName, "name", "", "The name of the transform")
	return cmd
}
