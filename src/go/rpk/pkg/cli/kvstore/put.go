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

func newPutCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partition   int32
		keyFormat   string
		valueFormat string
		ifExists    string
		ifMatches   string
	)
	cmd := &cobra.Command{
		Use:   "put [TOPIC] [KEY] [VALUE]",
		Short: "Put a key-value pair into a topic's key-value store",
		Long: `Put a key-value pair into a topic's key-value store.

This command stores a key-value pair in the key-value store of a specified topic.

The --partition flag targets a specific partition. If not set (default -1), the key
will be hashed using murmur2 to determine the partition.

The --key-format flag controls how the key argument is interpreted (utf8, hex, base64).

The --value-format flag controls how the value argument is interpreted (utf8, hex, base64).

Preconditions can be specified to make the put conditional:
  --if-exists true   - Put only succeeds if the key exists
  --if-exists false  - Put only succeeds if the key does not exist
  --if-matches HASH  - Put only succeeds if the existing value's SHA-256 hash matches

Examples:
  rpk kvstore put my-topic my-key my-value
  rpk kvstore put my-topic --partition 0 my-key my-value
  rpk kvstore put my-topic --key-format hex 6d796b6579 my-value
  rpk kvstore put my-topic --if-exists false my-key my-value
  rpk kvstore put my-topic --if-matches abc123... my-key new-value
`,
		Args: cobra.ExactArgs(3),
		Run: func(_ *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(WriteResult{}); ok {
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
			keyStr := args[1]
			valueStr := args[2]

			// Decode key and value from input formats
			decodedKey, err := decodeInput(keyStr, keyFormat)
			out.MaybeDie(err, "failed to decode key: %v", err)
			decodedValue, err := decodeInput(valueStr, valueFormat)
			out.MaybeDie(err, "failed to decode value: %v", err)

			// Parse preconditions
			precondition, err := parsePrecondition(ifExists, ifMatches)
			out.MaybeDie(err, "failed to parse preconditions: %v", err)

			// Build request
			put := KVStorePut{
				Key:          decodedKey,
				Value:        decodedValue,
				Precondition: precondition,
			}
			reqBody := KVStoreWriteRequest{
				Puts: []KVStorePut{put},
			}

			// Create kvstore client and make request
			client := newKVStoreClient(p)
			ctx := context.Background()
			_, err = client.Write(ctx, topic, partition, reqBody)
			out.MaybeDie(err, "failed to put key: %v", err)

			// Create result
			result := WriteResult{
				Operation: "put",
				Topic:     topic,
				Partition: partition,
				KeyCount:  1,
				Success:   true,
			}

			printWriteResult(f, result, os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Target partition (default -1 means hash key with murmur2)")
	cmd.Flags().StringVar(&keyFormat, "key-format", "utf8", "Key format for input (utf8, hex, base64)")
	cmd.Flags().StringVar(&valueFormat, "value-format", "utf8", "Value format for input (utf8, hex, base64)")
	cmd.Flags().StringVar(&ifExists, "if-exists", "", "Precondition: 'true' if key must exist, 'false' if key must not exist")
	cmd.Flags().StringVar(&ifMatches, "if-matches", "", "Precondition: SHA-256 hex hash that existing value must match")
	return cmd
}

func printWriteResult(f config.OutFormatter, result WriteResult, w io.Writer) {
	if isText, _, formatted, err := f.Format(result); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", formatted)
		return
	}

	// Text format
	fmt.Fprintf(w, "Successfully performed %s on %d key(s) in topic '%s' (partition: %d)\n",
		result.Operation, result.KeyCount, result.Topic, result.Partition)
}
