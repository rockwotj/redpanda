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

func newGetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partition   int32
		keyFormat   string
		valueFormat string
	)
	cmd := &cobra.Command{
		Use:   "get [TOPIC] [KEY...]",
		Short: "Get a value from a topic's key-value store",
		Long: `Get a value from a topic's key-value store.

This command retrieves the value(s) associated with key(s) from the key-value store
of a specified topic. Multiple keys can be specified.

The --partition flag targets a specific partition. If not set (default -1), the
command will operate on all partitions or use murmur2 hash to determine the partition.

The --key-format flag controls how keys are interpreted from arguments and formatted
in the output (utf8, hex, base64).

The --value-format flag controls how values are formatted in the output (utf8, hex, base64).

Examples:
  rpk kvstore get my-topic my-key
  rpk kvstore get my-topic key1 key2 key3
  rpk kvstore get my-topic --partition 0 my-key
  rpk kvstore get my-topic --key-format hex 6d796b6579
  rpk kvstore get my-topic --value-format base64 my-key
`,
		Args: cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]GetResult{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			// Validate formats
			err = validateFormat(keyFormat)
			out.MaybeDie(err, "invalid key format: %v", err)
			err = validateFormat(valueFormat)
			out.MaybeDie(err, "invalid value format: %v", err)

			topic := args[0]
			keys := args[1:]

			// Decode keys from input format
			decodedKeys, err := decodeKeys(keys, keyFormat)
			out.MaybeDie(err, "failed to decode keys: %v", err)

			// Build request
			reqBody := KVStoreGetRequest{
				Keys: decodedKeys,
			}

			// Create kvstore client
			client, err := newKVStoreClient(p, fs)
			out.MaybeDie(err, "failed to create kvstore client: %v", err)
			ctx := context.Background()

			// Determine which partitions to query
			var partitions []int32
			if partition == -1 {
				// Query all partitions
				partitionCount, err := getTopicPartitionCount(ctx, fs, p, topic)
				out.MaybeDie(err, "failed to get partition count: %v", err)
				partitions = make([]int32, partitionCount)
				for i := int32(0); i < partitionCount; i++ {
					partitions[i] = i
				}
			} else {
				// Query specific partition
				partitions = []int32{partition}
			}

			// Make requests to all target partitions and aggregate results
			// Use a map to deduplicate results (prioritize found keys)
			resultMap := make(map[string]GetResult)
			for _, decodedKey := range decodedKeys {
				// Initialize all keys as not found
				keyStr, err := encodeOutput(decodedKey, keyFormat)
				out.MaybeDie(err, "failed to encode key: %v", err)
				resultMap[keyStr] = GetResult{
					Key:   keyStr,
					Value: nil,
					Found: false,
				}
			}

			for _, p := range partitions {
				respBody, err := client.Get(ctx, topic, p, reqBody)
				out.MaybeDie(err, "failed to get keys from partition %d: %v", p, err)

				// Merge results, prioritizing found values
				for _, result := range respBody.Results {
					keyStr, err := encodeOutput(result.Key, keyFormat)
					out.MaybeDie(err, "failed to encode key: %v", err)

					var valStr *string
					found := result.Value != nil
					if found {
						encoded, err := encodeOutput(*result.Value, valueFormat)
						out.MaybeDie(err, "failed to encode value: %v", err)
						valStr = &encoded
					}

					// Only update if we found the key (or if it was not found before)
					existingResult := resultMap[keyStr]
					if found || !existingResult.Found {
						resultMap[keyStr] = GetResult{
							Key:   keyStr,
							Value: valStr,
							Found: found,
						}
					}
				}
			}

			// Convert map to ordered slice (matching input order)
			results := make([]GetResult, 0, len(decodedKeys))
			for _, decodedKey := range decodedKeys {
				keyStr, err := encodeOutput(decodedKey, keyFormat)
				out.MaybeDie(err, "failed to encode key: %v", err)
				if result, exists := resultMap[keyStr]; exists {
					results = append(results, result)
				}
			}

			printGetResults(f, results, os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Target partition (default -1 means all partitions)")
	cmd.Flags().StringVar(&keyFormat, "key-format", "utf8", "Key format for input and output (utf8, hex, base64)")
	cmd.Flags().StringVar(&valueFormat, "value-format", "utf8", "Value format for output (utf8, hex, base64)")
	return cmd
}

func printGetResults(f config.OutFormatter, results []GetResult, w io.Writer) {
	if isText, _, formatted, err := f.Format(results); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", formatted)
		return
	}

	// Text format
	tw := out.NewTableTo(w, "KEY", "VALUE", "FOUND")
	defer tw.Flush()
	for _, r := range results {
		value := "<not found>"
		if r.Value != nil {
			value = *r.Value
		}
		tw.Print(r.Key, value, r.Found)
	}
}
