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
	"encoding/binary"
	"errors"

	"github.com/rockwotj/redpanda/src/go/sdk"
	"github.com/rockwotj/redpanda/src/go/sdk/sr"
)

var c sr.SchemaRegistryClient

func main() {
	c = sr.NewClient()
	redpanda.OnRecordWritten(avroToJsonTransform)
}

func avroToJsonTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	v := e.Record().Value
	if len(v) < 5 || v[0] != 0 {
		return nil, errors.New("invalid schema registry header")
	}
	id := binary.BigEndian.Uint32(v[1:5])
	s, err := c.LookupSchemaById(sr.SchemaId(id))
	if err != nil {
		return nil, err
	}
	v = v[5:]
	ex, err := DeserializeExampleFromSchema(bytes.NewReader(v), string(s.Schema))
	if err != nil {
		return nil, err
	}
	j, err := ex.MarshalJSON()
	return []redpanda.Record{{
		Key:   e.Record().Key,
		Value: j,
	}}, err
}
