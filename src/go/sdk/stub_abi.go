// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
