/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */


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
