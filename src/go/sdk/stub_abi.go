// Copyright 2023 Redpanda Data
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

//go:build !tinygo

// This file only exists for IDE/LSP support (which doesn't understand tinygo)

// This package contains the interface for Redpanda's wasm transform golang SDK.
package redpanda

import (
	"io"
	"time"
)

type OnTransformFn func(e TransformEvent) error

func OnTransform(fn OnTransformFn) {}

type TransformEvent interface {
	Record() InputRecord
}

type Headers interface {
	Get(key string) string
	Keys() []string
}

type InputRecord interface {
	Key() io.Reader
	Value() io.Reader

	Headers() Headers
	Timestamp() time.Time
	Offset() int64
}

type OutputRecord interface {
	Key() io.Writer
	Value() io.Writer
	AppendHeader(name string, value string) error
}

func CreateOutputRecord() (OutputRecord, error) {
	panic("stub")
}
