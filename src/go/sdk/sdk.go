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

package redpanda

import (
	"time"
)

// The ABI that our SDK provides. Redpanda executes this function to determine the protocol contract to execute.

type EventErrorCode int32

const (
	evtSuccess       = EventErrorCode(0)
	evtConfigError   = EventErrorCode(1)
	evtUserError     = EventErrorCode(2)
	evtInternalError = EventErrorCode(3)
)

type inputBatchHandle int32
type inputRecordHandle int32

// OnTransformFn is a callback to transform records
type OnTransformFn func(e TransformEvent) ([]Record, error)

var userTransformFunction OnTransformFn = nil

// OnTransform should be called in a package's `main` function to register the transform function that will be applied.
func OnTransform(fn OnTransformFn) {
	userTransformFunction = fn
}

// Headers are optional key/value pairs that are passed along with
// records.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

type Record struct {
	Key   []byte
	Value []byte

	Attrs     RecordAttrs
	Headers   []RecordHeader
	Timestamp time.Time
	Offset    int64
}

type RecordAttrs struct {
	attr uint8
}

func (a RecordAttrs) TimestampType() int8 {
	if a.attr&0b1000_0000 != 0 {
		return -1
	}
	return int8(a.attr&0b0000_1000) >> 3
}

func (a RecordAttrs) CompressionType() uint8 {
	return a.attr & 0b0000_0111
}

func (a RecordAttrs) IsTransactional() bool {
	return a.attr&0b0001_0000 != 0
}

func (a RecordAttrs) IsControl() bool {
	return a.attr&0b0010_0000 != 0
}

type TransformEvent interface {
	Record() Record
}

type transformEvent struct {
	record Record
}

func (e *transformEvent) Record() Record {
	return e.record
}
