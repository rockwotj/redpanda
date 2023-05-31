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
)

const baseWasmEndpoint = "/v1/wasm/"
const deploySuffix = "/deploy"
const undeploySuffix = "/deploy"
const listSuffix = "list"

// Deploy a wasm transform to a cluster
func (a *AdminAPI) DeployWasmTransform(ctx context.Context, topic string, file io.Reader) error {
	return a.sendAny(ctx, http.MethodPost, baseWasmEndpoint+"kafka/"+topic+deploySuffix, file, nil)
}

// These are the wasm functions available
type LiveWasmFunction struct {
	Namespace string `json:"ns"`
	Topic     string `json:"topic"`
	Function  string `json:"function"`
}

// List wasm transforms in a cluster
func (a *AdminAPI) ListWasmTransforms(ctx context.Context) ([]LiveWasmFunction, error) {
	var f []LiveWasmFunction
	err := a.sendAny(ctx, http.MethodGet, baseWasmEndpoint+listSuffix, nil, f)
	return f, err
}

// Delete a wasm transforms in a cluster
func (a *AdminAPI) DeleteWasmTransforms(ctx context.Context, topic string) error {
	return a.sendAny(ctx, http.MethodDelete, baseWasmEndpoint+"kafka/"+topic+undeploySuffix, nil, nil)
}
