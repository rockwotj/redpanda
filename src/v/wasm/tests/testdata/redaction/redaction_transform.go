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
	"io"

	"github.com/mailru/easyjson"
	"redpanda.com/wasm/model"
	"redpanda.com/wasm/redpanda"

	_ "github.com/wasilibs/nottinygc"
)

func main() {
	redpanda.OnTransform(onTransform)
}

// Temporary buffer so that GC isn't invoked so much!
var buf = make([]byte, 4096)

func onTransform(e redpanda.TransformEvent) error {
	var jsonData model.GithubRepoData
	err := easyjson.UnmarshalFromReader(e.Record.Value, &jsonData)
	if err != nil {
		return err
	}
	// Remove gravatars as a redaction step for science
	jsonData.Owner.GravatarID = "<redacted>"
	jsonData.Organization.GravatarID = "<redacted>"
	jsonData.Source.Owner.GravatarID = "<redacted>"
	jsonData.Parent.Owner.GravatarID = "<redacted>"

	output, err := redpanda.CreateOutputRecord()
	if err != nil {
		return err
	}

	// copy over the key
	_, err = io.CopyBuffer(output.Key, e.Record.Key, buf)
	if err != nil {
		return err
	}
		
	// write out the value
	_, err = easyjson.MarshalToWriter(&jsonData, output.Value)
	if err != nil {
		return err
	}

	return nil
}
