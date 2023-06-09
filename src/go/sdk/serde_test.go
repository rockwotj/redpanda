package redpanda

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/rockwotj/redpanda/src/go/sdk/internal/rwbuf"
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
	b := rwbuf.New(0)
	r.serialize(b)
	output := Record{}
	err := output.deserialize(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r, output) {
		t.Fatalf("%#v != %#v", r, output)
	}
}
