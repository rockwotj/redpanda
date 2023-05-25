// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is for usage in documentation and IDE/LSP support
// It should be kept in line with the actual ABI public interfaces.
//go:build !tinygo

package redpanda

import (
	"io"
	"time"
)

// OnTransformFn is a callback to transform records
type OnTransformFn func(e TransformEvent) error

// OnTransform should be called in a package's `main` function to register the transform function that will be applied.
func OnTransform(fn OnTransformFn) {}

// TransformEvent is generated for each record passing through the system.
type TransformEvent interface {
	// Access the record that is associated with this event.
	Record() InputRecord
}

// Headers are optional key/value pairs that are passed along with
// records.
type Headers interface {
	// Lookup a key by name, returning empty string if not found.
	Get(key string) string
	// Return all of the keys that exist in a record's Headers.
	Keys() []string
}

// InputRecord is a record that was written by a producer.
type InputRecord interface {
	// The reader for the key that the producer supplied when writing this record.
	Key() io.Reader
	// The reader for the payload associated with the record.
	Value() io.Reader
	// The headers that the producer attached to this record.
	Headers() Headers
	// The timestamp used for the record. These timestamps can be set by a client or by the broker.
	Timestamp() time.Time
	// The offset into the log at which this record was written.
	Offset() int64
}

// OutputRecord is a record that can be output by a transform.
// OutputRecords can be created using `CreateOutputRecord`.
type OutputRecord interface {
	// The writer for the optional key that can be supplied with this record.
	Key() io.Writer
	// The writer for this record's value.
	Value() io.Writer
	// AppendHeader adds a header to this record. There is no deduplication that is done.
	AppendHeader(name string, value string) error
}

// Used to create an output record, the output record is flushed to the partition after the `OnTransformFn` returns.
func CreateOutputRecord() (OutputRecord, error) {
	panic("stub")
}
