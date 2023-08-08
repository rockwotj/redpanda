package main

import (
	"bytes"

	goavro "github.com/linkedin/goavro/v2"
	"github.com/rockwotj/redpanda/src/go/sdk"
	"github.com/rockwotj/redpanda/src/go/sdk/internal/transcoding_example/avro"
)

var codec *goavro.Codec

func main() {
	// Register your transform function
	redpanda.OnRecordWritten(doTransform)
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	i, err := avro.DeserializeInterop(bytes.NewReader(e.Record().Value))
	if err != nil {
		return nil, err
	}
	b, err := i.MarshalJSON()
	return []redpanda.Record{{
		Key:   e.Record().Key,
		Value: b,
	}}, err
}
