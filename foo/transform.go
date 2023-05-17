
package main

import (
	"io"

	"github.com/rockwotj/redpanda/src/go/sdk"
)

func main() {
	redpanda.OnTransform(onTransform)
}

// Temporary buffer so that GC isn't invoked so much!
var buf = make([]byte, 4096)

// This is an example of the "identity" transform that does nothing.
// You'll want to replace this with something that modifies the key
// or value.
func onTransform(e redpanda.TransformEvent) error {
	output, err := redpanda.CreateOutputRecord()
	if err != nil {
		return err
	}

	// copy over the key
	_, err = io.CopyBuffer(output.Key(), e.Record().Key(), buf)
	if err != nil {
		return err
	}
		
	// copy over the value
	_, err = io.CopyBuffer(output.Value(), e.Record().Value(), buf)
	if err != nil {
		return err
	}

	// copy over the headers
	for _, k := range(e.Record().Headers().Keys()) {
		v := e.Record().Headers().Get(k)
		err = output.AppendHeader(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}
