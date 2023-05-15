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

// Deploy a wasm transform to a cluster
func (a *AdminAPI) DeployWasm(ctx context.Context, topic string, file io.Reader) error {
	return a.sendAny(ctx, http.MethodPost, baseWasmEndpoint + "kafka/" + topic + deploySuffix, file, nil)
}
