// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
)

const prefix = "PREFIX: "

func testAddingPrefixToLines(t *testing.T, input, want string) {
	// Test a new different buffer sizes to ensure it all works correctly
	sizes := []int{1, 7, 16, 32, 64}
	if len(input) > 0 {
		sizes = append(sizes, len(input))
	}
	for _, size := range sizes {
		o := new(bytes.Buffer)
		w := utils.NewLinePrefixWriter(o, prefix)
		r := strings.NewReader(input)
		amt, err := io.CopyBuffer(w, r, make([]byte, size))
		if err != nil {
			t.Error("unexpected err:", err)
			break
		}
		if int(amt) != len(input) {
			t.Error("unexpected number of bytes written in testcase: ", size, " got: ", amt, " want: ", len(input))
			break
		}
		got := o.String()
		if want != got {
			t.Errorf("unexpected output in testcase: %d got: %q want: %q", size, got, want)
			break
		}
	}
}

func TestEmptyPrefixWriter(t *testing.T) {
	testAddingPrefixToLines(t, "", "")
}

func TestSingleLinePrefixWriter(t *testing.T) {
	testAddingPrefixToLines(t, "hello world", "PREFIX: hello world")
}

func TestMultiLinePrefixWriter(t *testing.T) {
	testAddingPrefixToLines(t, "hello\nworld\nfoo\nbar", "PREFIX: hello\nPREFIX: world\nPREFIX: foo\nPREFIX: bar")
}

func TestTrailingNewlinePrefixWrite(t *testing.T) {
	testAddingPrefixToLines(t, "hello\nworld\n", "PREFIX: hello\nPREFIX: world\n")
}
