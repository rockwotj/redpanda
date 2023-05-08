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
