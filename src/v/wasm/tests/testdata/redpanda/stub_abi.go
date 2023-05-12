/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

//go:build !tinygo

package redpanda

import (
	"io"
)


type OnTransformFn func(e TransformEvent) error
var userTransformFunction OnTransformFn = nil

func OnTransform(fn OnTransformFn) {
	userTransformFunction = fn
}

type TransformEvent struct {
	Record InputRecord
}

type Headers struct {
}

func (h *Headers) Get(key string) string {
	panic("stub")
}

func (h *Headers) Keys() []string {
	panic("stub")
}

type InputRecord struct {
	Key   io.Reader
	Value io.Reader

	Headers Headers
}


type OutputRecord struct {
	Key   io.Writer
	Value io.Writer
}

func CreateOutputRecord() (*OutputRecord, error) {
	panic("stub")
}

func (o *OutputRecord) AppendHeader(name string, value string) error {
	panic("stub")
}
