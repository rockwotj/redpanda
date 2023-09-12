// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

const (
	baseWasmEndpoint = "/v1/transform/"
	deploySuffix     = "deploy"
	deleteSuffix     = "delete"
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

// ClusterWasmTransform is the metadata about an individual transform.
type ClusterWasmTransform struct {
	InputTopic   string            `json:"input_topic"`
	OutputTopics []string          `json:"output_topics"`
	Name         string            `json:"name"`
	Env          map[string]string `json:"env,omitempty"`
}

// Deploy a wasm transform to a cluster.
func (a *AdminAPI) DeployWasmTransform(ctx context.Context, t ClusterWasmTransform, file io.Reader) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	// The format of these bytes is a little akward, there is a json header on the wasm source
	// that specifies the details.
	body := io.MultiReader(bytes.NewReader(b), file)
	return a.sendToLeader(ctx, http.MethodPost, generatePath(deploySuffix), body, nil)
}

// List wasm transforms in a cluster.
func (a *AdminAPI) ListWasmTransforms(ctx context.Context) ([]ClusterWasmTransform, error) {
	var f []ClusterWasmTransform
	err := a.sendToLeader(ctx, http.MethodGet, generatePath(listSuffix), nil, &f)
	return f, err
}

// Delete a wasm transforms in a cluster.
func (a *AdminAPI) DeleteWasmTransform(ctx context.Context, t ClusterWasmTransform) error {
	return a.sendToLeader(ctx, http.MethodPost, generatePath(deleteSuffix), t, nil)
}
