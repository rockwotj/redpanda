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

//go:build tinygo

package redpanda

//go:wasm-module redpanda
//export read_record
//extern read_record
func readRecord(h inputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export write_record
//extern write_record
func writeRecord(buf *byte, len int) int
