package redpanda

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var (
	baseTimestamp = int64(9)
	baseOffset    = int64(42)
)

func makeRandomHeaders(n int) []RecordHeader {
	h := make([]RecordHeader, n)
	for i := 0; i < n; i++ {
		k := make([]byte, rand.Intn(32))
		rand.Read(k)
		v := make([]byte, rand.Intn(32))
		rand.Read(v)
		h[i] = RecordHeader{Key: k, Value: v}
	}
	return h
}

func makeRandomRecord() Record {
	k := make([]byte, rand.Intn(32))
	rand.Read(k)
	v := make([]byte, rand.Intn(32))
	rand.Read(v)
	return Record{
		Key:       k,
		Value:     v,
		Attrs:     RecordAttrs{0},
		Headers:   makeRandomHeaders(2),
		Timestamp: time.UnixMilli(baseTimestamp + 9),
		Offset:    baseOffset + 6,
	}
}

func TestRoundTrip(t *testing.T) {
	r := makeRandomRecord()
	buf := &bytes.Buffer{}
	header := batchHeader{
		handle:               0,
		baseOffset:           42,
		baseTimestamp:        9,
		recordCount:          1,
		partitionLeaderEpoch: 1,
		attributes:           0,
		lastOffsetDelta:      0,
		maxTimestamp:         9,
		producerId:           3287,
		producerEpoch:        1,
		baseSequence:         1,
	}
	l, err := r.serialize(buf, header, r)
	if err != nil {
		t.Fatal(err)
	}
	buf = bytes.NewBuffer(buf.Bytes()[:l])
	output := Record{}
	err = deserializeRecord(buf, header, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r, output) {
		t.Fatalf("%#v != %#v", r, output)
	}
}
