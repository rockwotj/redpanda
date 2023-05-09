//go:build tinygo

package redpanda

import (
	"errors"
	"fmt"
	"io"
	"unsafe"
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

const evtSuccess = EventErrorCode(0)
const evtConfigError = EventErrorCode(1)
const evtUserError = EventErrorCode(2)

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
	err := userTransformFunction(TransformEvent{InputRecord{h, &keyReader{h}, &valueReader{h}, Headers{h}}})
	if err != nil {
		fmt.Println("transforming record failed: %v", err)
		return evtUserError
	}
	return evtSuccess
}

type TransformEvent struct {
	Record InputRecord
}

type Headers struct {
	handle inputRecordHandle
}

func (h *Headers) Get(key string) string {
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

func (h *Headers) Keys() []string {
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

type InputRecord struct {
	handle inputRecordHandle

	Key   io.Reader
	Value io.Reader

	Headers Headers
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
func offset(h inputRecordHandle) uint64

//go:wasm-module redpanda
//export timestamp
//extern timestamp
func timestamp(h inputRecordHandle) uint64

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

type OutputRecord struct {
	handle outputRecordHandle

	Key   io.Writer
	Value io.Writer
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

func CreateOutputRecord() (*OutputRecord, error) {
	handle := createOutputRecord()
	if handle < 0 {
		return nil, errors.New("Unable to create output record")
	}
	return &OutputRecord{handle, &keyWriter{handle}, &valueWriter{handle}}, nil
}

func (o *OutputRecord) AppendHeader(name string, value string) error {
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
