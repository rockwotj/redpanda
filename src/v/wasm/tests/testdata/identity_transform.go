package main

import (
	"fmt"
	"io"

	"redpanda.com/wasm/redpanda"
	_ "github.com/wasilibs/nottinygc"
)

func main() {
	redpanda.OnTransform(onTransform)
	fmt.Println("Working")
}

// Temporary buffer so that GC isn't invoked so much!
var buf = make([]byte, 4096)

func onTransform(e redpanda.TransformEvent) error {

	output, err := redpanda.CreateOutputRecord()
	if err != nil {
		return err
	}

	// copy over the key
	_, err = io.CopyBuffer(output.Key, e.Record.Key, buf)
	if err != nil {
		return err
	}
		
	// copy over the value
	_, err = io.CopyBuffer(output.Value, e.Record.Value, buf)
	if err != nil {
		return err
	}

	// copy over the headers
	for _, k := range(e.Record.Headers.Keys()) {
		v := e.Record.Headers.Get(k)
		err = output.AppendHeader(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}
