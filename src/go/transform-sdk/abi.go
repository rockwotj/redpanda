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

//go:build tinygo

package redpanda

import (
	"unsafe"
)

// These are the host functions that go allows for wasm functions that are imported.
// See: https://github.com/golang/go/issues/59149

//go:wasmimport redpanda_transform read_batch_header
func readRecordHeader(
	h inputBatchHandle,
	baseOffset unsafe.Pointer,
	recordCount unsafe.Pointer,
	partitionLeaderEpoch unsafe.Pointer,
	attributes unsafe.Pointer,
	lastOffsetDelta unsafe.Pointer,
	baseTimestamp unsafe.Pointer,
	maxTimestamp unsafe.Pointer,
	producerId unsafe.Pointer,
	producerEpoch unsafe.Pointer,
	baseSequence unsafe.Pointer,
) int32

//go:wasmimport redpanda_transform read_record
func readRecord(h inputRecordHandle, buf unsafe.Pointer, len int32) int32

//go:wasmimport redpanda_transform write_record
func writeRecord(buf unsafe.Pointer, len int32) int32
