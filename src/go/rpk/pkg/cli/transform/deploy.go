// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package transform

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
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
		env          []string
	)
	cmd := &cobra.Command{
		Use:   "deploy [WASM]",
		Short: "Deploy a transform",
		Long: `Deploy a transform.

When run in the same directory as a transform.yaml it will read the configuration file,
then look for a .wasm file with the same name as your project. If the input and output
topics are specified in the configuration file, those will be used, otherwise the topics
can be specified on the command line using the --input-topic and --output-topic flags.

Wasm files can also be deployed directly without a transform.yaml file by specifying it
like so:

rpk transform deploy transform.wasm --name myTransform
		`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			cfg, cfgErr := project.LoadCfg(fs)
			if functionName == "" {
				out.MaybeDie(cfgErr, "unable to find the transform, are you in the same directory as the %q?", project.ConfigFileName)
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
				if cfgErr == nil && cfg.InputTopic != "" {
					inputTopic = cfg.InputTopic
				} else {
					inputTopic, err = out.Prompt("Select an input topic:")
					out.MaybeDie(err, "no input topic: %v", err)
				}
			}
			if outputTopic == "" {
				if cfgErr == nil && cfg.OutputTopic != "" {
					outputTopic = cfg.OutputTopic
				} else {
					outputTopic, err = out.Prompt("Select an output topic:")
					out.MaybeDie(err, "no output topic: %v", err)
				}
			}

			envvars, err := splitEnvVars(env)
			out.MaybeDie(err, "invalid environment variable: %v", err)
			if cfgErr != nil {
				m := map[string]string{}
				// flags override the config file
				for k, v := range cfg.Env {
					m[k] = v
				}
				for k, v := range envvars {
					m[k] = v
				}
				envvars = m
			}

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "missing %q: %v did you run `rpk transform build`", path, err)

			transforms, err := api.ListWasmTransforms(cmd.Context())
			out.MaybeDie(err, "unable to list existing transforms: %v", err)

			// Validate that this transform is unique in name and topics that it uses
			for _, t := range transforms {
				if t.FunctionName == functionName && t.InputTopic == inputTopic && t.OutputTopic == outputTopic {
					// We're redeploying!
					break
				}
				if t.FunctionName == functionName {
					out.Die("a transform named %q from %q into %q already exists", functionName, t.InputTopic, t.OutputTopic)
				}
				if t.InputTopic == inputTopic || t.OutputTopic == inputTopic {
					out.Die("topic %q is already attached to a transform", t.InputTopic)
				}
				if t.OutputTopic == outputTopic || t.InputTopic == outputTopic {
					out.Die("topic %q is already attached to a transform", t.OutputTopic)
				}
			}

			t := admin.ClusterWasmTransform{
				Namespace:    "kafka",
				InputTopic:   inputTopic,
				OutputTopic:  outputTopic,
				FunctionName: functionName,
				Env:          envvars,
			}
			err = api.DeployWasmTransform(cmd.Context(), t, bytes.NewReader(file))
			out.MaybeDie(err, "unable to deploy transfrom %q: %v", path, err)

			fmt.Println("Deploy successful!")

			// Do we support saving after the first deploy?
			// cfg, err := loadCfg(fs)
			// s := false
			// if !rd && err == nil {
			// 	s, err = out.Confirm("save input and output topics to %s for the next deploy of %q", configFileName, functionName)
			// 	if s && err != nil {
			// 		cfg.InputTopic = inputTopic
			// 		cfg.OutputTopic = outputTopic
			// 		err = saveCfg(fs, cfg)
			// 		out.MaybeDie(err, "unable to save %q: %v", configFileName, err)
			// 	}
			// }
		},
	}
	cmd.Flags().StringVarP(&inputTopic, "input-topic", "i", "", "The input topic to apply the transform to")
	cmd.Flags().StringVarP(&outputTopic, "output-topic", "o", "", "The output topic to write the transform results to")
	cmd.Flags().StringVar(&functionName, "name", "", "The name of the transform")
	cmd.Flags().StringArrayVar(&env, "env-var", []string{}, "Specify an environment variable in the form of KEY=VALUE")
	return cmd
}

func splitEnvVars(raw []string) (map[string]string, error) {
	e := map[string]string{}
	for _, r := range raw {
		i := strings.IndexByte(r, '=')
		if i == -1 {
			return nil, fmt.Errorf("missing value for %q", r)
		}
		k := r[:i]
		if k == "" {
			return nil, fmt.Errorf("missing key for %q", r)
		}
		v := r[i+1:]
		if v == "" {
			return nil, fmt.Errorf("empty value for %q", r)
		}
		e[k] = v
	}
	return e, nil
}
