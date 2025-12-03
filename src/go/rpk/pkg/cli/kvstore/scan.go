// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kvstore

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newScanCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partition   int32
		startKey    string
		endKey      string
		keyFormat   string
		valueFormat string
		limit       int32
	)
	cmd := &cobra.Command{
		Use:   "scan [TOPIC]",
		Short: "Scan keys in a topic's key-value store",
		Long: `Scan keys in a topic's key-value store.

This command performs a range scan over keys in the key-value store of a specified topic.
Keys are returned in lexicographical byte order.

The --partition flag targets a specific partition. If not set (default -1), the
command will scan all partitions.

The --start-key flag specifies the starting key (inclusive) for the scan. If not
specified, scanning starts from the first key.

The --end-key flag specifies the ending key (exclusive) for the scan. If not
specified, scanning continues through the last key.

The --key-format flag controls how start/end keys are interpreted and how keys are
formatted in the output (utf8, hex, base64).

The --value-format flag controls how values are formatted in the output (utf8, hex, base64).

The --limit flag sets the maximum number of entries to return (default 500, max 1000).

Examples:
  rpk kvstore scan my-topic
  rpk kvstore scan my-topic --partition 0
  rpk kvstore scan my-topic --start-key key1 --end-key key9
  rpk kvstore scan my-topic --key-format hex --start-key 6b657931
  rpk kvstore scan my-topic --limit 100
  rpk kvstore scan my-topic --value-format base64
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]ScanEntry{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			// Validate formats
			err = validateFormat(keyFormat)
			out.MaybeDie(err, "invalid key format: %v", err)
			err = validateFormat(valueFormat)
			out.MaybeDie(err, "invalid value format: %v", err)

			// Validate limit
			if limit <= 0 {
				out.Die("limit must be greater than 0")
			}
			if limit > 1000 {
				out.Die("limit cannot exceed 1000")
			}

			topic := args[0]

			// Build request
			reqBody := KVStoreScanRequest{
				Limit: limit,
			}

			// Decode start key if provided
			if startKey != "" {
				decodedStart, err := decodeInput(startKey, keyFormat)
				out.MaybeDie(err, "failed to decode start-key: %v", err)
				reqBody.StartKey = &decodedStart
			}

			// Decode end key if provided
			if endKey != "" {
				decodedEnd, err := decodeInput(endKey, keyFormat)
				out.MaybeDie(err, "failed to decode end-key: %v", err)
				reqBody.EndKey = &decodedEnd
			}

			// Create kvstore client and make request
			client, err := newKVStoreClient(p, fs)
			out.MaybeDie(err, "failed to create kvstore client: %v", err)
			ctx := context.Background()
			respBody, err := client.Scan(ctx, topic, partition, reqBody)
			out.MaybeDie(err, "failed to scan keys: %v", err)

			// Convert response to output format
			entries := make([]ScanEntry, 0, len(respBody.Entries))
			for _, entry := range respBody.Entries {
				keyStr, err := encodeOutput(entry.Key, keyFormat)
				out.MaybeDie(err, "failed to encode key: %v", err)
				valStr, err := encodeOutput(entry.Value, valueFormat)
				out.MaybeDie(err, "failed to encode value: %v", err)

				entries = append(entries, ScanEntry{
					Key:       keyStr,
					Value:     valStr,
					Partition: partition,
				})
			}

			printScanResults(f, entries, os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Target partition (default -1 means all partitions)")
	cmd.Flags().StringVar(&startKey, "start-key", "", "Starting key for scan (inclusive)")
	cmd.Flags().StringVar(&endKey, "end-key", "", "Ending key for scan (exclusive)")
	cmd.Flags().StringVar(&keyFormat, "key-format", "utf8", "Key format for input and output (utf8, hex, base64)")
	cmd.Flags().StringVar(&valueFormat, "value-format", "utf8", "Value format for output (utf8, hex, base64)")
	cmd.Flags().Int32Var(&limit, "limit", 500, "Maximum number of entries to return (max 1000)")
	return cmd
}

func printScanResults(f config.OutFormatter, entries []ScanEntry, w io.Writer) {
	if isText, _, formatted, err := f.Format(entries); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", formatted)
		return
	}

	// Text format
	if len(entries) == 0 {
		fmt.Fprintln(w, "No entries found")
		return
	}

	tw := out.NewTableTo(w, "KEY", "VALUE")
	defer tw.Flush()
	for _, e := range entries {
		tw.Print(e.Key, e.Value)
	}
}
