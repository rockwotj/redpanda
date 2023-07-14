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
	)

	command := &cobra.Command{
		Use:   "logs",
		Short: "Watch logs",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()

			re, err := regexp.Compile(filter)
			out.MaybeDie(err, "invalid filter %q: %v", filter, err)

			nodes, err := common.GetExistingNodes(c)
			if err != nil {
				return common.WrapIfConnErr(err)
			}
			if len(nodes) == 0 {
				fmt.Println("No Redpanda nodes detected - use `rpk container start` or check `docker ps` if you expected nodes")
				return nil
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
						r := utils.NewLineFilteredReader(lr, re)
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
		"follow",
		false,
		"Tail the logs continuously",
	)

	command.Flags().StringVar(
		&filter,
		"filter",
		"",
		"Filter for the logs",
	)

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
