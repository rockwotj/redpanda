// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"bytes"
	"io"
)

type prefixWriter struct {
	prefix                []byte
	underlying            io.Writer
	previousByteIsNewline bool
}

// NewLinePrefixWriter returns a writer that adds a prefix to every line written.
func NewLinePrefixWriter(w io.Writer, prefix string) io.Writer {
	return &prefixWriter{
		prefix:                []byte(prefix),
		underlying:            w,
		previousByteIsNewline: true,
	}
}

func (pw *prefixWriter) Write(payload []byte) (amt int, err error) {
	for len(payload) > 0 {
		if pw.previousByteIsNewline {
			_, err = pw.underlying.Write(pw.prefix)
			if err != nil {
				return amt, err
			}
			pw.previousByteIsNewline = false
		}
		i := bytes.IndexByte(payload, '\n')
		if i < 0 {
			a, err := pw.underlying.Write(payload)
			amt += a
			if err != nil {
				return amt, err
			}
			break
		}
		toFlush := payload[:i+1]
		a, err := pw.underlying.Write(toFlush)
		amt += a
		if err != nil {
			return amt, err
		}
		pw.previousByteIsNewline = true
		payload = payload[i+1:]
	}
	return amt, nil
}
