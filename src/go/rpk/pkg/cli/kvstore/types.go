// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kvstore

// KVStoreWriteRequest is a request to atomically write a batch of data.
// Maps to redpanda.core.rest.v1.KVStoreWriteRequest proto message.
type KVStoreWriteRequest struct {
	Puts    []KVStorePut    `json:"puts,omitempty"`
	Deletes []KVStoreDelete `json:"deletes,omitempty"`
}

// KVStorePut inserts or updates a key-value pair.
type KVStorePut struct {
	Key          []byte               `json:"key"`
	Value        []byte               `json:"value"`
	Precondition *KVStorePrecondition `json:"precondition,omitempty"`
}

// KVStoreDelete removes a key-value pair.
type KVStoreDelete struct {
	Key          []byte               `json:"key"`
	Precondition *KVStorePrecondition `json:"precondition,omitempty"`
}

// KVStorePrecondition specifies conditions that must be satisfied for a write
// operation to succeed.
type KVStorePrecondition struct {
	IfExists  *KVStoreIfExists  `json:"ifExists,omitempty"`
	IfMatches *KVStoreIfMatches `json:"ifMatches,omitempty"`
}

// KVStoreIfExists checks whether a key exists or not.
type KVStoreIfExists struct {
	Exists bool `json:"exists"`
}

// KVStoreIfMatches checks whether a key's value matches a specific hash.
type KVStoreIfMatches struct {
	SHA256Hash string `json:"sha256Hash"`
}

// KVStoreWriteResponse is returned when a write operation completes successfully.
type KVStoreWriteResponse struct{}

// KVStoreGetRequest retrieves specific keys from a KVStore partition.
type KVStoreGetRequest struct {
	Keys [][]byte `json:"keys"`
}

// KVStoreGetResponse contains the requested key-value entries.
type KVStoreGetResponse struct {
	Results []KVStoreLookupResult `json:"results"`
}

// KVStoreLookupResult is a lookup result for a key in the KVStore.
type KVStoreLookupResult struct {
	Key   []byte  `json:"key"`
	Value *[]byte `json:"value,omitempty"`
}

// KVStoreScanRequest performs a range scan over keys in a KVStore partition.
type KVStoreScanRequest struct {
	StartKey *[]byte `json:"startKey,omitempty"`
	EndKey   *[]byte `json:"endKey,omitempty"`
	Limit    int32   `json:"limit,omitempty"`
}

// KVStoreScanResponse contains the entries found during a scan operation.
type KVStoreScanResponse struct {
	Entries []KVStoreEntry `json:"entries"`
}

// KVStoreEntry is a key/value pair stored in the topic partition's KVStore.
type KVStoreEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Output structs for formatted printing

// GetResult represents a single get result for formatted output.
type GetResult struct {
	Key   string  `json:"key" yaml:"key"`
	Value *string `json:"value,omitempty" yaml:"value,omitempty"`
	Found bool    `json:"found" yaml:"found"`
}

// ScanEntry represents a single scan entry for formatted output.
type ScanEntry struct {
	Key       string `json:"key" yaml:"key"`
	Value     string `json:"value" yaml:"value"`
	Partition int32  `json:"partition" yaml:"partition"`
}

// WriteResult represents the result of a put or delete operation.
type WriteResult struct {
	Operation string `json:"operation" yaml:"operation"`
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
	KeyCount  int    `json:"key_count" yaml:"key_count"`
	Success   bool   `json:"success" yaml:"success"`
}
