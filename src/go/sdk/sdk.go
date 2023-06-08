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
	"bytes"
	"encoding/binary"
	"errors"
	"time"
)

// The ABI that our SDK provides. Redpanda executes this function to determine the protocol contract to execute.
//export redpanda_abi_version
func redpandaAbiVersion() int32 {
	return 2
}

type EventErrorCode int32

const (
	evtSuccess       = EventErrorCode(0)
	evtConfigError   = EventErrorCode(1)
	evtUserError     = EventErrorCode(2)
	evtInternalError = EventErrorCode(3)
)

type inputBatchHandle int64
type inputRecordHandle int32

// OnTransformFn is a callback to transform records
type OnTransformFn func(e TransformEvent) ([]Record, error)

var userTransformFunction OnTransformFn = nil

// OnTransform should be called in a package's `main` function to register the transform function that will be applied.
func OnTransform(fn OnTransformFn) {
	userTransformFunction = fn
}

type batchHeader struct {
	handle               inputBatchHandle
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
	currentBatchHeader    batchHeader    = batchHeader{handle: -1}
	scratchRecordBuf      *bytes.Buffer  = &bytes.Buffer{}
	incomingRecordHeaders []RecordHeader = nil
	e                     transformEvent
)

//export redpanda_on_record
func redpandaOnRecord(bh inputBatchHandle, rh inputRecordHandle, batchHeaderSize, recordSize int) EventErrorCode {
	if userTransformFunction == nil {
		println("Invalid configuration, there is no registered user transform function")
		return evtConfigError
	}
	if currentBatchHeader.handle != bh {
		err := readRecordHeader(
			bh,
			&currentBatchHeader.baseOffset,
			&currentBatchHeader.recordCount,
			&currentBatchHeader.partitionLeaderEpoch,
			&currentBatchHeader.attributes,
			&currentBatchHeader.lastOffsetDelta,
			&currentBatchHeader.baseTimestamp,
			&currentBatchHeader.maxTimestamp,
			&currentBatchHeader.producerId,
			&currentBatchHeader.producerEpoch,
			&currentBatchHeader.baseSequence,
		)
		if err != 0 {
			return evtInternalError
		}
	}
	scratchRecordBuf.Reset()
	scratchRecordBuf.Grow(recordSize)
	buf := scratchRecordBuf.Bytes()
	amt := readRecord(rh, &buf[0], recordSize)
	if amt < recordSize {
		return evtInternalError
	}
	err := deserializeRecord(scratchRecordBuf, currentBatchHeader, &e.record)
	if err != nil {
		println("deserializing record failed:", err.Error())
		return evtInternalError
	}
	r, err := userTransformFunction(&e)
	if err != nil {
		println("transforming record failed:", err.Error())
		return evtUserError
	}
	if r == nil {
		return evtSuccess
	}
	for i := 0; i < len(r); i++ {
		l, err := r[i].serialize(scratchRecordBuf, currentBatchHeader, e.record)
		if err != nil {
			println("serializing record failed:", err.Error())
			return evtInternalError
		}
		writeRecord(&scratchRecordBuf.Bytes()[0], l)
	}
	return evtSuccess
}

func deserializeRecord(b *bytes.Buffer, bh batchHeader, r *Record) error {
	rs, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	if rs != int64(b.Len()) {
		return errors.New("Record size mismatch")
	}
	attr, err := b.ReadByte()
	if err != nil {
		return err
	}
	td, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	od, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	kl, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	var k []byte = nil
	if kl >= 0 {
		k = b.Next(int(kl))
	}
	vl, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	var v []byte = nil
	if vl >= 0 {
		v = b.Next(int(vl))
	}
	hc, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	if incomingRecordHeaders == nil || len(incomingRecordHeaders) < int(hc) {
		incomingRecordHeaders = make([]RecordHeader, hc)
	}
	for i := 0; i < int(hc); i++ {
		kl, err := binary.ReadVarint(b)
		if err != nil {
			return err
		}
		var k []byte = nil
		if kl >= 0 {
			k = b.Next(int(kl))
		}
		vl, err := binary.ReadVarint(b)
		if err != nil {
			return err
		}
		var v []byte = nil
		if vl >= 0 {
			v = b.Next(int(vl))
		}
		incomingRecordHeaders[i] = RecordHeader{
			Key:   k,
			Value: v,
		}
	}
	r.Key = k
	r.Value = v
	r.Attrs.attr = attr
	r.Headers = incomingRecordHeaders
	// TODO: if logAppendTime timestamp is MaxTimestamp not first + delta
	r.Timestamp = time.UnixMilli(bh.baseTimestamp + td)
	r.Offset = bh.baseOffset + od
	return nil
}

func writeVarint(b *bytes.Buffer, offset int, v int64) int {
	additionalLength := (offset + binary.MaxVarintLen64) - b.Len()
	if additionalLength > 0 {
		b.Grow(additionalLength)
	}
	offset += binary.PutVarint(b.Bytes()[offset:offset+binary.MaxVarintLen64], v)
	return offset
}
func writeBytes(b *bytes.Buffer, offset int, v []byte) int {
	additionalLength := (offset + len(v)) - b.Len()
	if additionalLength > 0 {
		b.Grow(additionalLength)
	}
	offset += copy(b.Bytes()[offset:offset+len(v)], v)
	return offset
}

func (r Record) serialize(b *bytes.Buffer, bh batchHeader, original Record) (int, error) {
	b.Reset()
	// Reserve space for writing the record length, which we'll write at the end
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b.WriteByte(0)
	}
	offset := binary.MaxVarintLen64
	err := b.WriteByte(r.Attrs.attr)
	if err != nil {
		return 0, err
	}
	offset += 1

	var ts int64
	if r.Timestamp.IsZero() {
		// For an unset timesamp, just use the original timestamp
		ts = original.Timestamp.UnixMilli() - bh.baseTimestamp
	} else {
		ts = r.Timestamp.UnixMilli() - bh.baseTimestamp
	}
	offset = writeVarint(b, offset, ts)
	// Always use the original offset, note the func can override these values
	// so the host still has to verify this.
	offset = writeVarint(b, offset, original.Offset-bh.baseOffset)
	if r.Key != nil {
		offset = writeVarint(b, offset, int64(len(r.Key)))
		offset = writeBytes(b, offset, r.Key)
	} else {
		offset = writeVarint(b, offset, -1)
	}
	if r.Value != nil {
		offset = writeVarint(b, offset, int64(len(r.Value)))
		offset = writeBytes(b, offset, r.Value)
	} else {
		offset = writeVarint(b, offset, -1)
	}
	offset = writeVarint(b, offset, int64(len(r.Headers)))
	for _, h := range r.Headers {
		if h.Key != nil {
			offset = writeVarint(b, offset, int64(len(h.Key)))
			offset = writeBytes(b, offset, h.Key)
		} else {
			offset = writeVarint(b, offset, -1)
		}
		if h.Value != nil {
			offset = writeVarint(b, offset, int64(len(h.Value)))
			offset = writeBytes(b, offset, h.Value)
		} else {
			offset = writeVarint(b, offset, -1)
		}
	}
	// Write the full length at the beginning of the buffer (we reserved this space above)
	offset -= binary.MaxVarintLen64
	so := writeVarint(b, 0, int64(offset))
	shift := binary.MaxVarintLen64 - so
	// Copy the varint we just wrote so there is no gap, and consume the start offset
	copy(
		b.Bytes()[shift:shift+so],
		b.Bytes()[:so],
	)
	b.Next(shift)
	return so + offset, nil
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
