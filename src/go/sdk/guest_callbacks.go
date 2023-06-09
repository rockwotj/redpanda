package redpanda

import (
	"time"

	"github.com/rockwotj/redpanda/src/go/sdk/internal/rwbuf"
)

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
	currentHeader batchHeader  = batchHeader{handle: -1}
	inbuf         *rwbuf.RWBuf = rwbuf.New(2048)
	outbuf        *rwbuf.RWBuf = rwbuf.New(2048)
	e             transformEvent
)

//export redpanda_abi_version
func redpandaAbiVersion() int32 {
	return 2
}

//export redpanda_on_record
func redpandaOnRecord(bh inputBatchHandle, rh inputRecordHandle, recordSize int, currentRelativeOutputOffset int) EventErrorCode {
	if userTransformFunction == nil {
		println("Invalid configuration, there is no registered user transform function")
		return evtConfigError
	}
	if currentHeader.handle != bh {
		currentHeader.handle = bh
		errno := readRecordHeader(
			bh,
			&currentHeader.baseOffset,
			&currentHeader.recordCount,
			&currentHeader.partitionLeaderEpoch,
			&currentHeader.attributes,
			&currentHeader.lastOffsetDelta,
			&currentHeader.baseTimestamp,
			&currentHeader.maxTimestamp,
			&currentHeader.producerId,
			&currentHeader.producerEpoch,
			&currentHeader.baseSequence,
		)
		if errno != 0 {
			println("Failed to read batch header")
			return evtInternalError
		}
	}
	inbuf.Reset()
	inbuf.EnsureSize(recordSize)
	amt := readRecord(rh, inbuf.WriterBufPtr(), recordSize)
	inbuf.AdvanceWriter(amt)
	if amt != recordSize {
		println("reading record failed with errno:", amt)
		return evtInternalError
	}
	err := e.record.deserialize(inbuf)
	if err != nil {
		println("deserializing record failed:", err.Error())
		return evtInternalError
	}
	// Save the original timestamp for output records
	ot := e.Record().Timestamp
	// Fix up the offsets to be absolute values
	e.record.Offset += currentHeader.baseOffset
	if e.record.Attrs.TimestampType() == 0 {
		e.record.Timestamp = time.UnixMilli(e.record.Timestamp.UnixMilli() + currentHeader.baseTimestamp)
	} else {
		e.record.Timestamp = time.UnixMilli(currentHeader.maxTimestamp)
	}
	rs, err := userTransformFunction(&e)
	if err != nil {
		println("transforming record failed:", err.Error())
		return evtUserError
	}
	if rs == nil {
		return evtSuccess
	}
	for i, r := range rs {
		// Because the previous record in the batch could have
		// output multiple records, we need to account for this,
		// by adjusting the offset accordingly.
		r.Offset = int64(currentRelativeOutputOffset + i)
		// Keep the same timestamp as the input record.
		r.Timestamp = ot

		outbuf.Reset()
		r.serialize(outbuf)
		b := outbuf.ReadAll()
		amt := writeRecord(&b[0], len(b))
		if amt != len(b) {
			println("writing record failed with errno:", amt)
			return evtInternalError
		}
	}
	return evtSuccess
}
