// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redpanda

import (
	"strconv"
	"time"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/go/transform-sdk/internal/rwbuf"
)

type batchHeader struct {
	baseOffset           int64
	recordCount          int
	partitionLeaderEpoch int
	attributes           int16
	lastOffsetDelta      int
	baseTimestamp        int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int
}

// Cache a bunch of objects to not GC
var (
	currentHeader batchHeader  = batchHeader{}
	inbuf         *rwbuf.RWBuf = rwbuf.New(128)
	outbuf        *rwbuf.RWBuf = rwbuf.New(128)
	e             writeEvent
	offsets       [64]int32
)

// run our transformation loop
func process(userTransformFunction OnRecordWrittenCallback) {
	if userTransformFunction == nil {
		panic("Invalid configuration, there is a nil registered user transform function")
	}
	checkAbiVersion()
	for {
		processBatch(userTransformFunction)
	}
}

// process and transform a single batch
func processBatch(userTransformFunction OnRecordWrittenCallback) {
	bufSize := int(readBatchHeader(
		unsafe.Pointer(&currentHeader.baseOffset),
		unsafe.Pointer(&currentHeader.recordCount),
		unsafe.Pointer(&currentHeader.partitionLeaderEpoch),
		unsafe.Pointer(&currentHeader.attributes),
		unsafe.Pointer(&currentHeader.lastOffsetDelta),
		unsafe.Pointer(&currentHeader.baseTimestamp),
		unsafe.Pointer(&currentHeader.maxTimestamp),
		unsafe.Pointer(&currentHeader.producerId),
		unsafe.Pointer(&currentHeader.producerEpoch),
		unsafe.Pointer(&currentHeader.baseSequence),
	))
	if bufSize < 0 {
		panic("failed to read batch header errno: " + strconv.Itoa(bufSize))
	}

	for i := 0; i < int(currentHeader.recordCount); i++ {
		// TODO Don't hardcode this size
		var headerCount int32
		inbuf.Reset()
		inbuf.EnsureSize(bufSize)
		var timestamp int64
		amt := int(readNextRecord(
			unsafe.Pointer(&e.record.Attrs.attr),
			unsafe.Pointer(&timestamp),
			unsafe.Pointer(&e.record.Offset),
			unsafe.Pointer(&headerCount),
			unsafe.Pointer(&offsets[0]),
			int32(len(offsets)),
			unsafe.Pointer(inbuf.WriterBufPtr()),
			int32(bufSize),
		))
		// Assign the timestamp value to the record
		e.record.Timestamp = time.UnixMilli(timestamp)
		inbuf.AdvanceWriter(amt)
		if amt < 0 {
			panic("reading record failed with errno: " + strconv.Itoa(amt) + " buffer size: " + strconv.Itoa(bufSize))
		}
		headerCount *= 2
		err := e.record.deserializePayload(offsets[:headerCount+2], inbuf)
		if err != nil {
			panic("deserializing record failed: " + err.Error())
		}
		rs, err := userTransformFunction(&e)
		if err != nil {
			panic("transforming record failed: " + err.Error())
		}
		if rs == nil {
			continue
		}
		for _, r := range rs {
			outbuf.Reset()
			r.serializePayload(offsets[:(1+len(r.Headers))*2], outbuf)
			b := outbuf.ReadAll()
			// Write the record back out to the broker
			errno := int(writeRecord(
				unsafe.Pointer(&offsets[0]), int32((1+len(r.Headers))*2),
				unsafe.Pointer(&b[0]), int32(len(b)),
			))
			if errno != 0 {
				panic("writing record failed with errno: " + strconv.Itoa(errno))
			}
		}
	}
}
