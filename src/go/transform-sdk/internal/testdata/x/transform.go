package main

import (
	"bytes"

	"github.com/redpanda-data/redpanda/src/go/transform-sdk"
	"omb-wasm/avro"
)

var b = bytes.Buffer{}

func main() {
	// Register your transform function
	redpanda.ProcessWrittenRecords(doTransform)
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	i := avro.NewInterop()
	err := i.UnmarshalJSON([]byte(string(e.Record().Value)))
	if err != nil {
		println("error reading json:", err)
		// OMB sends some dummy messages sometimes to test things, so just copy it out verbatium
		return []redpanda.Record{e.Record()}, nil
	}
	b.Reset()
	err = i.Serialize(&b)
	if err != nil {
		return nil, err
	}
	return []redpanda.Record{{
		Key:     e.Record().Key,
		Value:   b.Bytes(),
		Headers: e.Record().Headers,
	}}, err
}
