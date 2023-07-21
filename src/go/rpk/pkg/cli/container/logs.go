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
	"errors"
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
		tail   bool
		filter string
		level  string
	)

	command := &cobra.Command{
		Use:   "logs [LOGGERS...]",
		Short: "View broker logs",
		Args:  cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()

			ll, err := common.ParseLogLevel(level)
			out.MaybeDie(err, "invalid log level: %q", level)
			filters := []*regexp.Regexp{}
			for _, arg := range args {
				filters = append(filters, regexp.MustCompile(regexp.QuoteMeta(arg)))
			}
			if filter != "" {
				re, err := regexp.Compile(filter)
				out.MaybeDie(err, "invalid filter %q: %v", filter, err)
				filters = append(filters, re)
			}

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
							Follow:     tail,
						})
						if err != nil {
							return common.WrapIfConnErr(err)
						}
						defer lr.Close()
						info, err := c.ContainerInspect(ctx, cid)
						if err != nil {
							return common.WrapIfConnErr(err)
						}
						pr, pw := io.Pipe()
						g, _ := errgroup.WithContext(ctx)
						g.Go(func() error {
							r := common.NewLogsFilterReader(pr, ll, filters...)
							w := utils.NewLinePrefixWriter(os.Stdout, name+" ")
							_, err := io.Copy(w, r)
							return err
						})
						g.Go(func() error {
							if !info.Config.Tty {
								_, err = stdcopy.StdCopy(pw, pw, lr)
							} else {
								_, err = io.Copy(pw, lr)
							}
							return pw.CloseWithError(err)
						})
						return common.WrapIfConnErr(g.Wait())
					})
				}
				return g.Wait()
			}
			if tail {
				err = withInterruptHandling(dumpLogs)
			} else {
				ctx, _ := common.DefaultCtx()
				err = dumpLogs(ctx)
			}
			return common.WrapIfConnErr(err)
		},
	}

	command.Flags().BoolVar(
		&tail,
		"follow",
		false,
		"output new entries as they are appended",
	)

	command.Flags().StringVar(
		&filter,
		"filter",
		"",
		"Filter log messages according to the regular expression",
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
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
