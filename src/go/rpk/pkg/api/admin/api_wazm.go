// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// NOTES: ending this file in _wasm makes golang think this is a wasm only file similar to _linux, _windows etc.

package admin

import (
	"context"
	"io"
	"net/http"
	"net/url"
)

const (
	baseWasmEndpoint = "/v1/wasm/"
	deploySuffix     = "deploy"
	listSuffix       = "list"
)

func generatePath(suffix string) string {
	return generatePathQuery(suffix, url.Values{})
}

func generatePathQuery(suffix string, params url.Values) string {
	if len(params) == 0 {
		return baseWasmEndpoint + suffix
	}
	return baseWasmEndpoint + suffix + "?" + params.Encode()
}

// Deploy a wasm transform to a cluster
func (a *AdminAPI) DeployWasmTransform(ctx context.Context, inputTopic string, outputTopic string, functionName string, file io.Reader) error {
	params := url.Values{
		"namespace":     {"kafka"},
		"input_topic":   {inputTopic},
		"output_topic":  {outputTopic},
		"function_name": {functionName},
	}
	return a.sendToLeader(ctx, http.MethodPost, generatePathQuery(deploySuffix, params), file, nil)
}

// These are the wasm functions available
type ClusterWasmTransform struct {
	Namespace    string `json:"ns"`
	InputTopic   string `json:"input_topic"`
	OutputTopic  string `json:"output_topic"`
	FunctionName string `json:"function_name"`
	Status       string `json:"status"`
}

// List wasm transforms in a cluster
func (a *AdminAPI) ListWasmTransforms(ctx context.Context) ([]ClusterWasmTransform, error) {
	var f []ClusterWasmTransform
	err := a.sendToLeader(ctx, http.MethodGet, generatePath(listSuffix), nil, &f)
	return f, err
}

// Delete a wasm transforms in a cluster
func (a *AdminAPI) UndeployWasmTransform(ctx context.Context, t ClusterWasmTransform) error {
	params := url.Values{
		"namespace":     {t.Namespace},
		"input_topic":   {t.InputTopic},
		"output_topic":  {t.OutputTopic},
		"function_name": {t.FunctionName},
	}
	return a.sendToLeader(ctx, http.MethodDelete, generatePathQuery(deploySuffix, params), nil, nil)
}
