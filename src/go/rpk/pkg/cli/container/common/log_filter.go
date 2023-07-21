// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// LogLevel is a corresponding level for a log line in Redpanda.
type LogLevel int

const (
	LogLevelTrace LogLevel = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// ParseLogLevel returns a corresponding log level for a string.
func ParseLogLevel(str string) (LogLevel, error) {
	switch strings.ToUpper(str) {
	case LogLevelTrace.String():
		return LogLevelTrace, nil
	case LogLevelDebug.String():
		return LogLevelDebug, nil
	case LogLevelInfo.String():
		return LogLevelInfo, nil
	case LogLevelWarn.String():
		return LogLevelWarn, nil
	case LogLevelError.String():
		return LogLevelError, nil
	default:
		return LogLevelTrace, fmt.Errorf("unknown log level %q", str)
	}
}

// String returns the string representation of a log level.
func (ll LogLevel) String() string {
	switch ll {
	case LogLevelTrace:
		return "TRACE"
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		panic("unknown log level: " + strconv.Itoa(int(ll)))
	}
}

func (ll LogLevel) toRegexpFilter() *regexp.Regexp {
	levels := []string{}
	for l := ll; l <= LogLevelError; l++ {
		levels = append(levels, l.String())
	}
	return regexp.MustCompile("^(" + strings.Join(levels, "|") + ")")
}

type logsFilterReader struct {
	logLevelFilter     *regexp.Regexp
	messageFilters     []*regexp.Regexp
	underlying         *bufio.Scanner
	currentLine        []byte
	currentLineIsMatch bool
}

// NewLogsFilterReader creates a new logs filter that filters for the current log level
// along with any additional filters on the log's message in the form of a regexp.
func NewLogsFilterReader(r io.Reader, level LogLevel, filters ...*regexp.Regexp) io.Reader {
	return &logsFilterReader{
		logLevelFilter:     level.toRegexpFilter(),
		messageFilters:     filters,
		underlying:         bufio.NewScanner(r),
		currentLine:        nil,
		currentLineIsMatch: false,
	}
}

var logLineStartRegexp = LogLevelTrace.toRegexpFilter()

func (r *logsFilterReader) readCurrentLineIntoBuf(p []byte) (n int) {
	n = copy(p, r.currentLine)
	r.currentLine = r.currentLine[n:]
	if len(r.currentLine) == 0 {
		r.currentLine = nil
	}
	return
}

func (r *logsFilterReader) Read(p []byte) (n int, err error) {
	if r.currentLineIsMatch && r.currentLine != nil {
		return r.readCurrentLineIntoBuf(p), nil
	}
	for r.underlying.Scan() {
		// if we found a new line then reset our state
		if logLineStartRegexp.Match(r.underlying.Bytes()) {
			r.currentLine = nil
			r.currentLineIsMatch = false
		}
		// append our current line
		r.currentLine = append(r.currentLine, r.underlying.Bytes()...)
		r.currentLine = append(r.currentLine, '\n')
		// are we matching an existing log line? then copy it out
		if r.currentLineIsMatch {
			return r.readCurrentLineIntoBuf(p), nil
		}
		// otherwise check if this line matches our filter yet
		m := r.logLevelFilter.Match(r.currentLine)
		// Message filters only check against the message
		i := bytes.Index(r.currentLine, []byte{']', ' '}) + 1
		for _, re := range r.messageFilters {
			m = m && re.Match(r.currentLine[i:])
		}
		r.currentLineIsMatch = m
		// oh it matches? great let's copy it out
		if r.currentLineIsMatch {
			return r.readCurrentLineIntoBuf(p), nil
		}
	}
	err = r.underlying.Err()
	if err == nil {
		err = io.EOF
	}
	return
}
