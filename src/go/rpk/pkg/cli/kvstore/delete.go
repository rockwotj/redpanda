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

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partition int32
		keyFormat string
		ifExists  string
		ifMatches string
	)
	cmd := &cobra.Command{
		Use:   "delete [TOPIC] [KEY...]",
		Short: "Delete a key from a topic's key-value store",
		Long: `Delete a key from a topic's key-value store.

This command removes key(s) and associated value(s) from the key-value store
of a specified topic. Multiple keys can be specified.

The --partition flag targets a specific partition. If not set (default -1), the
command will operate on all partitions or use murmur2 hash to determine the partition.

The --key-format flag controls how keys are interpreted from arguments (utf8, hex, base64).

Preconditions can be specified to make the delete conditional:
  --if-exists true   - Delete only succeeds if the key exists
  --if-exists false  - Delete only succeeds if the key does not exist
  --if-matches HASH  - Delete only succeeds if the existing value's SHA-256 hash matches

Examples:
  rpk kvstore delete my-topic my-key
  rpk kvstore delete my-topic key1 key2 key3
  rpk kvstore delete my-topic --partition 0 my-key
  rpk kvstore delete my-topic --key-format hex 6d796b6579
  rpk kvstore delete my-topic --if-exists true my-key
  rpk kvstore delete my-topic --if-matches abc123... my-key
`,
		Args: cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(WriteResult{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			// Validate format
			err = validateFormat(keyFormat)
			out.MaybeDie(err, "invalid key format: %v", err)

			topic := args[0]
			keys := args[1:]

			// Decode keys from input format
			decodedKeys, err := decodeKeys(keys, keyFormat)
			out.MaybeDie(err, "failed to decode keys: %v", err)

			// Parse preconditions
			precondition, err := parsePrecondition(ifExists, ifMatches)
			out.MaybeDie(err, "failed to parse preconditions: %v", err)

			// Build request with deletes for each key
			deletes := make([]KVStoreDelete, len(decodedKeys))
			for i, key := range decodedKeys {
				deletes[i] = KVStoreDelete{
					Key:          key,
					Precondition: precondition,
				}
			}
			reqBody := KVStoreWriteRequest{
				Deletes: deletes,
			}

			// Create kvstore client and make request
			client := newKVStoreClient(p)
			ctx := context.Background()
			_, err = client.Write(ctx, topic, partition, reqBody)
			out.MaybeDie(err, "failed to delete keys: %v", err)

			// Create result
			result := WriteResult{
				Operation: "delete",
				Topic:     topic,
				Partition: partition,
				KeyCount:  len(reqBody.Deletes),
				Success:   true,
			}

			printWriteResult(f, result, os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Target partition (default -1 means all partitions or hash-based)")
	cmd.Flags().StringVar(&keyFormat, "key-format", "utf8", "Key format for input (utf8, hex, base64)")
	cmd.Flags().StringVar(&ifExists, "if-exists", "", "Precondition: 'true' if key must exist, 'false' if key must not exist")
	cmd.Flags().StringVar(&ifMatches, "if-matches", "", "Precondition: SHA-256 hex hash that existing value must match")
	return cmd
}
