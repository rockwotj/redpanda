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
		inputTopic string
		outputTopic string
		functionName string
	)
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy Wasm function",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if functionName == "" {
				functionName, err = out.Prompt("What is the function's name:")
				out.MaybeDie(err, "no input topic: %v", err)
			}
			if inputTopic == "" {
				inputTopic, err = out.Prompt("Select an input topic:")
				out.MaybeDie(err, "no input topic: %v", err)
			}
			if outputTopic == "" {
				outputTopic, err = out.Prompt("Select an output topic:")
				out.MaybeDie(err, "no input topic: %v", err)
			}
			path := args[0]
			ok, err := afero.Exists(fs, path)
			out.MaybeDie(err, "missing %q: %v", path, err)
			if !ok {
				out.Die("missing %q", path)
			}

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "missing %q: %v did you run `rpk wasm build`", path, err)

			err = api.DeployWasmTransform(cmd.Context(), inputTopic, outputTopic, functionName, bytes.NewReader(file))
			out.MaybeDie(err, "unable to deploy wasm %q: %v", path, err)

			fmt.Println("Deploy successful!")
		},
	}
	cmd.Flags().StringVarP(&inputTopic, "input-topic", "i", "", "The input topic to apply the transform to")
	cmd.Flags().StringVarP(&outputTopic, "output-topic", "o", "", "The output topic to write the transform results to")
	cmd.Flags().StringVarP(&functionName, "name", "n", "", "The name of the transform")
	return cmd
}
