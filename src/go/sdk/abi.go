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

import (
	"errors"
	"fmt"
	"io"
	"unsafe"
	"time"
)

// Tinygo documentation on wasi: https://tinygo.org/docs/guides/webassembly/
// golang proper support is still work in progress.
func stringBytePtr(str string) *byte {
	if len(str) == 0 {
		return nil
	}
	bt := *(*[]byte)(unsafe.Pointer(&str))
	return &bt[0]
}

//export redpanda_abi_version
func redpandaAbiVersion() int32 {
	return 1
}

type EventErrorCode int32

const (
	evtSuccess     = EventErrorCode(0)
	evtConfigError = EventErrorCode(1)
	evtUserError   = EventErrorCode(2)
)

type inputRecordHandle int32

type OnTransformFn func(e TransformEvent) error

var userTransformFunction OnTransformFn = nil

func OnTransform(fn OnTransformFn) {
	userTransformFunction = fn
}

//export redpanda_on_record
func redpandaOnRecord(h inputRecordHandle) EventErrorCode {
	if userTransformFunction == nil {
		fmt.Println("Invalid configuration, there is no registered user transform function")
		return evtConfigError
	}
	err := userTransformFunction(&transformEvent{&inputRecord{h, &keyReader{h}, &valueReader{h}, &headers{h}}})
	if err != nil {
		fmt.Println("transforming record failed:", err)
		return evtUserError
	}
	return evtSuccess
}

type TransformEvent interface {
	Record() InputRecord
}

type transformEvent struct {
	record InputRecord
}

func (e *transformEvent) Record() InputRecord {
	return e.record
}

type Headers interface {
	Get(key string) string
	Keys() []string
}

type headers struct {
	handle inputRecordHandle
}

func (h *headers) Get(key string) string {
	idx := findHeaderByKey(h.handle, stringBytePtr(key), len(key))
	if idx < 0 {
		return ""
	}
	length := getHeaderValueLength(h.handle, idx)
	if length < 0 {
		return ""
	}
	// TODO: This should be pooled
	sharedBuf := make([]byte, length)
	result := getHeaderValue(h.handle, idx, &sharedBuf[0], len(sharedBuf))
	if result < 0 {
		return ""
	}
	return string(sharedBuf)
}

func (h *headers) Keys() []string {
	size := numHeaders(h.handle)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		length := getHeaderKeyLength(h.handle, i)
		if length < 0 {
			keys[i] = ""
			break
		}
		// TODO: This should be pooled
		sharedBuf := make([]byte, length)
		result := getHeaderKey(h.handle, i, &sharedBuf[0], len(sharedBuf))
		if result < 0 {
			keys[i] = ""
			break
		}
		keys[i] = string(sharedBuf)
	}
	return keys
}

type keyReader struct {
	handle inputRecordHandle
}

func (kr *keyReader) Read(b []byte) (n int, err error) {
	result := readKey(kr.handle, &b[0], len(b))
	if result < 0 {
		return 0, errors.New("Failed to read key")
	} else if result == 0 {
		return result, io.EOF
	} else {
		return result, nil
	}
}

type valueReader struct {
	handle inputRecordHandle
}

func (vr *valueReader) Read(b []byte) (n int, err error) {
	result := readValue(vr.handle, &b[0], len(b))
	if result < 0 {
		return 0, errors.New("Failed to read value")
	} else if result == 0 {
		return result, io.EOF
	} else {
		return result, nil
	}
}

type InputRecord interface {
	Key() io.Reader
	Value() io.Reader

	Headers() Headers
	Timestamp() time.Time
	Offset() int64
}

type inputRecord struct {
	handle inputRecordHandle

	key   io.Reader
	value io.Reader

	headers Headers
}

func (r *inputRecord) Key() io.Reader {
	return r.key
}

func (r *inputRecord) Value() io.Reader {
	return r.value
}

func (r *inputRecord) Headers() Headers {
	return r.headers
}

func (r *inputRecord) Timestamp() time.Time {
	return time.UnixMilli(timestamp(r.handle))
}

func (r *inputRecord) Offset() int64 {
	return offset(r.handle)
}

//go:wasm-module redpanda
//export read_key
//extern read_key
func readKey(h inputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export read_value
//extern read_value
func readValue(h inputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export num_headers
//extern num_headers
func numHeaders(h inputRecordHandle) int

//go:wasm-module redpanda
//export find_header_by_key
//extern find_header_by_key
func findHeaderByKey(h inputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export get_header_key_length
//extern get_header_key_length
func getHeaderKeyLength(h inputRecordHandle, index int) int

//go:wasm-module redpanda
//export get_header_value_length
//extern get_header_value_length
func getHeaderValueLength(h inputRecordHandle, index int) int

//go:wasm-module redpanda
//export get_header_key
//extern get_header_key
func getHeaderKey(h inputRecordHandle, index int, buf *byte, len int) int

//go:wasm-module redpanda
//export get_header_value
//extern get_header_value
func getHeaderValue(h inputRecordHandle, index int, buf *byte, len int) int

//go:wasm-module redpanda
//export offset
//extern offset
func offset(h inputRecordHandle) int64

//go:wasm-module redpanda
//export timestamp
//extern timestamp
func timestamp(h inputRecordHandle) int64

type outputRecordHandle int32

//go:wasm-module redpanda
//export create_output_record
//extern create_output_record
func createOutputRecord() outputRecordHandle

//go:wasm-module redpanda
//export write_key
//extern write_key
func writeKey(handle outputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export write_value
//extern write_value
func writeValue(handle outputRecordHandle, buf *byte, len int) int

//go:wasm-module redpanda
//export append_header
//extern append_header
func appendHeader(handle outputRecordHandle, keyBuffer *byte, keyLength int, valueBuffer *byte, valueLength int) int

type OutputRecord interface {
	Key() io.Writer
	Value() io.Writer
	AppendHeader(name string, value string) error
}

type outputRecord struct {
	handle outputRecordHandle

	key   io.Writer
	value io.Writer
}

func (r *outputRecord) Key() io.Writer {
	return r.key
}

func (r *outputRecord) Value() io.Writer {
	return r.value
}

type keyWriter struct {
	handle outputRecordHandle
}

func (kw *keyWriter) Write(b []byte) (n int, err error) {
	result := writeKey(kw.handle, &b[0], len(b))
	if result < 0 {
		return 0, errors.New("Unable to write to key")
	} else {
		return result, nil
	}
}

type valueWriter struct {
	handle outputRecordHandle
}

func (vw *valueWriter) Write(b []byte) (n int, err error) {
	result := writeValue(vw.handle, &b[0], len(b))
	if result < 0 {
		return 0, errors.New("Unable to write to value")
	} else {
		return result, nil
	}
}

func CreateOutputRecord() (OutputRecord, error) {
	handle := createOutputRecord()
	if handle < 0 {
		return nil, errors.New("Unable to create output record")
	}
	return &outputRecord{handle, &keyWriter{handle}, &valueWriter{handle}}, nil
}

func (o *outputRecord) AppendHeader(name string, value string) error {
	errc := appendHeader(
		o.handle,
		stringBytePtr(name),
		len(name),
		stringBytePtr(value),
		len(value),
	)
	if errc < 0 {
		return errors.New(fmt.Sprintf("Unable to append header %s=%s [%d]", name, value, errc))
	} else {
		return nil
	}
}
