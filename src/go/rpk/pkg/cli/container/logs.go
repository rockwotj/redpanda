// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package container

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newLogsCommand() *cobra.Command {
	var (
		follow bool
		filter string
		level  string
		host   string
	)

	command := &cobra.Command{
		Use:   "logs",
		Short: "View broker logs",
		Args:  cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()

			var lre, sre, ure *regexp.Regexp
			switch level {
			case "trace":
				lre = nil
			case "debug":
				lre = regexp.MustCompile("^(DEBUG|INFO|WARN|ERROR)")
			case "info":
				lre = regexp.MustCompile("^(INFO|WARN|ERROR)")
			case "warn":
				lre = regexp.MustCompile("^(WARN|ERROR)")
			case "error":
				lre = regexp.MustCompile("^(ERROR)")
			default:
				out.Die("invalid level %q", level)
			}

			if len(args) > 0 {
				for i, arg := range args {
					args[i] = regexp.QuoteMeta(arg)
				}
				// TODO: be more strict in matching log lines here
				sre = regexp.MustCompile("] (" + strings.Join(args, "|") + ") -")
			}

			if filter != "" {
				ure, err = regexp.Compile(filter)
				out.MaybeDie(err, "invalid filter %q: %v", filter, err)
			}

			nodes, err := common.GetExistingNodes(c)
			if err != nil {
				return common.WrapIfConnErr(err)
			}
			if len(nodes) == 0 {
				fmt.Println("No Redpanda nodes detected - use `rpk container start` or check `docker ps` if you expected nodes")
				return nil
			}
			if host != "all" {
				idx, err := strconv.Atoi(host)
				if err != nil || idx < 0 || idx >= len(nodes) {
					out.Die("invalid node index: %q", host)
				}
				nodes = []*common.NodeState{nodes[idx]}
			}
			dumpLogs := func(ctx context.Context) error {
				g, ctx := errgroup.WithContext(ctx)
				for _, node := range nodes {
					cid := node.ContainerID
					name := common.Name(node.ID)
					g.Go(func() error {
						lr, err := c.ContainerLogs(ctx, cid, types.ContainerLogsOptions{
							Timestamps: false,
							ShowStdout: true,
							ShowStderr: true,
							Follow:     follow,
						})
						if err != nil {
							return common.WrapIfConnErr(err)
						}
						defer lr.Close()
						var r io.Reader = lr
						// TODO: This is naive as log lines can be emitted with newlines
						// We should instead group as a first stage, then send it down a
						// pipeline of regexp filters
						if lre != nil {
							r = utils.NewLineFilteredReader(r, lre)
						}
						if sre != nil {
							r = utils.NewLineFilteredReader(r, sre)
						}
						if ure != nil {
							r = utils.NewLineFilteredReader(lr, ure)
						}
						info, err := c.ContainerInspect(ctx, cid)
						if err != nil {
							return common.WrapIfConnErr(err)
						}
						w := utils.NewLinePrefixWriter(os.Stdout, name+" ")
						if !info.Config.Tty {
							_, err = stdcopy.StdCopy(w, w, r)
						} else {
							_, err = io.Copy(w, r)
						}
						return common.WrapIfConnErr(err)
					})
				}
				return g.Wait()
			}
			if follow {
				err = withInterruptHandling(dumpLogs)
			} else {
				ctx, _ := common.DefaultCtx()
				err = dumpLogs(ctx)
			}
			return common.WrapIfConnErr(err)
		},
	}

	command.Flags().BoolVar(
		&follow,
		"tail",
		false,
		"continuously watch and output new logs as they are emitted (default false)",
	)

	command.Flags().StringVar(
		&filter,
		"filter",
		"",
		"Filter for the logs",
	)

	command.Flags().StringVar(
		&host,
		"host",
		"all",
		"The host number to emit logs for (default all)",
	)

	command.Flags().StringVarP(&level, "level", "l", "trace", "log level to filter, any logs below this level will be hidden (error, warn, info, debug, trace)")

	return command
}

// Run a command that is indended to run forever and only stops with there is an error or the user sends SIGINT or similar signal.
func withInterruptHandling(fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	sigs := make(chan os.Signal, 3)
	defer close(sigs)
	done := make(chan any, 1)
	defer close(done)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	g.Go(func() error {
		select {
		case <-sigs:
			cancel()
		case <-done:
		}
		return nil
	})
	g.Go(func() error {
		err := fn(ctx)
		done <- nil
		return err
	})
	err := g.Wait()
	if err == context.Canceled {
		return nil
	}
	return err
}
