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
	"io"
	"github.com/rockwotj/redpanda/src/go/sdk"
)

// This example shows the basic usage of the package:
// Read the record then output it's key and value with no changes.
func Example_identityTransform() {
	redpanda.OnTransform(onTransform)
}

func onTransform(e redpanda.TransformEvent) error {
	output, err := redpanda.CreateOutputRecord()
	if err != nil {
		return err
	}
	_, err = io.Copy(output.Key(), e.Record().Key())
	if err != nil {
		return err
	}
	_, err = io.Copy(output.Key(), e.Record().Value())
	return err
}
