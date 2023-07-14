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
	"bufio"
	"bytes"
	"io"
	"regexp"
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

type filteredReader struct {
	re         *regexp.Regexp
	underlying *bufio.Scanner
	available  []byte
}

// NewLineFilteredReader returns a reader that filters each line to if it matches the provided regexp
//
// A newline is always added to the last line if there is a match
func NewLineFilteredReader(r io.Reader, re *regexp.Regexp) io.Reader {
	return &filteredReader{
		re,
		bufio.NewScanner(r),
		nil,
	}
}

func (fr *filteredReader) Read(p []byte) (n int, err error) {
	if fr.available != nil {
		n = copy(p, fr.available)
		fr.available = fr.available[n:]
		if len(fr.available) == 0 {
			fr.available = nil
		}
		return
	}
	for fr.underlying.Scan() {
		if fr.re.Match(fr.underlying.Bytes()) {
			b := fr.underlying.Bytes()
			b = append(b, '\n')
			n = copy(p, b)
			if n < len(b) {
				fr.available = b[n:]
			}
			return
		}
	}
	err = fr.underlying.Err()
	if err == nil {
		err = io.EOF
	}
	return
}
