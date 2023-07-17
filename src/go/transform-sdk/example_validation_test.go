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

// This example shows a filter that outputs only valid JSON into the
// output topic.
func Example_validationFilter() {
	redpanda.OnRecordWritten(filterValidJson)
}

func filterValidJson(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	v := []redpanda.Record{}
	if json.Valid(e.Record().Value) {
		v = append(v, e.Record())
	}
	return v, nil
}
