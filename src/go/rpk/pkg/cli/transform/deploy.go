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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/registry"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type FlagsConfig struct {
	inputTopic   string
	outputTopic  string
	functionName string
	env          []string
}

func (fc FlagsConfig) Apply(cfg *project.Config, fromRegistry bool) (err error) {
	if fc.functionName != "" {
		cfg.Name = fc.functionName
	}
	if cfg.Name == "" {
		return fmt.Errorf("missing transform name")
	}
	envvars, err := splitEnvVars(fc.env)
	if err != nil {
		return fmt.Errorf("invalid environment variable: %v", err)
	}
	m := map[string]string{}
	// flags override the config file
	if cfg.Env != nil {
		for k, v := range cfg.Env {
			m[k] = v
		}
	}
	for k, v := range envvars {
		m[k] = v
	}
	cfg.Env = m
	if fromRegistry {
		for k, v := range cfg.Env {
			if v == "<required>" {
				return fmt.Errorf("missing required environment variable %q", k)
			}
		}
	}

	if fc.inputTopic != "" {
		cfg.InputTopic = fc.inputTopic
	} else if cfg.InputTopic == "" {
		cfg.InputTopic, err = out.Prompt("Select an input topic:")
		if err != nil {
			return fmt.Errorf("no input topic %v", err)
		}
	}
	if fc.outputTopic != "" {
		cfg.OutputTopic = fc.outputTopic
	} else if cfg.OutputTopic == "" {
		cfg.OutputTopic, err = out.Prompt("Select an output topic:")
		if err != nil {
			return fmt.Errorf("no output topic %v", err)
		}
	}
	return nil
}

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var fc FlagsConfig

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

			arg := ""
			if len(args) == 1 {
				arg = args[0]
			}

			var (
				cfg  project.Config
				wasm io.Reader
			)

			if strings.HasPrefix(arg, "@") {
				reg, id, v, err := registry.ParseRegistryString(arg)
				out.MaybeDie(err, "invalid specifier: %v", err)
				cfg, wasm, err = registry.DownloadTransform(cmd.Context(), reg, id, v)
				out.MaybeDie(err, "%v", err)
			} else {
				cfg, wasm, err = localDeploy(fc, fs, arg)
				out.MaybeDie(err, "%v", err)
			}
			err = fc.Apply(&cfg, strings.HasPrefix(arg, "@"))
			out.MaybeDie(err, "%v", err)

			transforms, err := api.ListWasmTransforms(cmd.Context())
			out.MaybeDie(err, "unable to list existing transforms: %v", err)

			// Validate that this transform is unique in name and topics that it uses
			for _, t := range transforms {
				if t.FunctionName == cfg.Name && t.InputTopic == cfg.InputTopic && t.OutputTopic == cfg.OutputTopic {
					// We're redeploying!
					break
				}
				if t.FunctionName == cfg.Name {
					out.Die("a transform named %q from %q into %q already exists", cfg.Name, t.InputTopic, t.OutputTopic)
				}
				if t.InputTopic == cfg.InputTopic || t.OutputTopic == cfg.InputTopic {
					out.Die("topic %q is already attached to a transform", t.InputTopic)
				}
				if t.OutputTopic == cfg.OutputTopic || t.InputTopic == cfg.OutputTopic {
					out.Die("topic %q is already attached to a transform", t.OutputTopic)
				}
			}

			t := admin.ClusterWasmTransform{
				Namespace:    "kafka",
				InputTopic:   cfg.InputTopic,
				OutputTopic:  cfg.OutputTopic,
				FunctionName: cfg.Name,
				Env:          cfg.Env,
			}
			err = api.DeployWasmTransform(cmd.Context(), t, wasm)
			if he := (*admin.HTTPResponseError)(nil); errors.As(err, &he) {
				if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("unable to deploy transform %s: %s", cfg.Name, body.Message)
					}
				}
			}
			out.MaybeDie(err, "unable to deploy transfrom %s: %v", cfg.Name, err)

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
	cmd.Flags().StringVarP(&fc.inputTopic, "input-topic", "i", "", "The input topic to apply the transform to")
	cmd.Flags().StringVarP(&fc.outputTopic, "output-topic", "o", "", "The output topic to write the transform results to")
	cmd.Flags().StringVar(&fc.functionName, "name", "", "The name of the transform")
	cmd.Flags().StringArrayVar(&fc.env, "env-var", []string{}, "Specify an environment variable in the form of KEY=VALUE")
	return cmd
}

func localDeploy(fc FlagsConfig, fs afero.Fs, arg string) (project.Config, io.Reader, error) {
	cfg, cfgErr := project.LoadCfg(fs)
	if fc.functionName != "" {
		cfg.Name = fc.functionName
	}
	var path string
	if arg != "" {
		path = arg
	} else if cfg.Name != "" {
		path = fmt.Sprintf("%s.wasm", cfg.Name)
	} else if cfgErr != nil {
		return cfg, nil, fmt.Errorf("unable to find the transform, are you in the same directory as the %q?", project.ConfigFileName)
	} else {
		return cfg, nil, errors.New("missing transfrom name")
	}
	ok, err := afero.Exists(fs, path)
	if err != nil {
		return cfg, nil, fmt.Errorf("missing %q: %v", path, err)
	}
	if !ok {
		return cfg, nil, fmt.Errorf("missing %q", path)
	}
	file, err := afero.ReadFile(fs, path)
	if err != nil {
		return cfg, nil, fmt.Errorf("missing %q: %v did you run `rpk transform build`", path, err)
	}
	return cfg, bytes.NewReader(file), err

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
