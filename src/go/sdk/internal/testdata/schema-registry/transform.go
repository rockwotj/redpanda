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

package main

import (
	"bytes"

	"github.com/rockwotj/redpanda/src/go/sdk"
	"github.com/rockwotj/redpanda/src/go/sdk/sr"
)

var (
	c         sr.SchemaRegistryClient
	s         sr.Serde[*Example]
	topicName = "demo-topic"
)

func main() {
	println("creating new client")
	c = sr.NewClient()
	println("creating example")
	e := Example{}
	println("creating schema")
	c.CreateSchema(topicName+"-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: e.Schema(),
	})
	println("registering on record written")
	redpanda.OnRecordWritten(avroToJsonTransform)
}

// This is an example transform that converts avro->json using the avro schema specified in schema registry.
func avroToJsonTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	v := e.Record().Value
	ex := Example{}
	// Attempt to decode the value, if it's from a schema we don't know about then
	// look it up and then try to decode.
	err := s.Decode(v, &ex)
	if err == sr.ErrNotRegistered {
		id, err := sr.ExtractID(v)
		if err != nil {
			return nil, err
		}
		schema, err := c.LookupSchemaById(id)
		if err != nil {
			return nil, err
		}
		// Register the new schema
		s.Register(id, sr.DecodeFn[*Example](func(b []byte, e *Example) error {
			ex, err := DeserializeExampleFromSchema(
				bytes.NewReader(b),
				schema.Schema,
			)
			*e = ex
			return err
		}))
		// Now try and decode the value now that we've looked it up.
		if err = s.Decode(v, &ex); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	// Output the record as JSON.
	j, err := ex.MarshalJSON()
	return []redpanda.Record{{
		Key:   e.Record().Key,
		Value: j,
	}}, err
}
