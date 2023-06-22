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

package redpanda_test

import (
	"encoding/json"

	"github.com/rockwotj/redpanda/src/go/sdk"
)

// This example shows a transform that renames fields in a JSON object.
func Example_renameJsonFields() {
	redpanda.OnRecordWritten(myTransform)
}

type (
	// The input topic uses keys A, B and C
	Foo struct {
		A string
		B int
		C bool
	}
	// The output wants records with keys X, Y and Z
	Bar struct {
		X string
		Y int
		Z bool
	}
)

// myTransform writes Bar structs into the output topic, where the input topic
// contains Foo structs.
func myTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	var foo Foo
	err := json.Unmarshal(e.Record().Value, &foo)
	if err != nil {
		return nil, err
	}
	bar := Bar{
		X: foo.A,
		Y: foo.B,
		Z: foo.C,
	}
	v, err := json.Marshal(&bar)
	return []redpanda.Record{{Key: e.Record().Key, Value: v}}, err
}
