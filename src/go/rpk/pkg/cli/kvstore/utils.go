// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kvstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// decodeInput decodes a string input based on the specified format.
// Supported formats: utf8, hex, base64
func decodeInput(input, format string) ([]byte, error) {
	switch strings.ToLower(format) {
	case "utf8", "":
		return []byte(input), nil
	case "hex":
		decoded, err := hex.DecodeString(input)
		if err != nil {
			return nil, fmt.Errorf("failed to decode hex: %v", err)
		}
		return decoded, nil
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(input)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %v", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported format %q, must be one of: utf8, hex, base64", format)
	}
}

// encodeOutput encodes bytes to a string based on the specified format.
// Supported formats: utf8, hex, base64
func encodeOutput(data []byte, format string) (string, error) {
	switch strings.ToLower(format) {
	case "utf8", "":
		return string(data), nil
	case "hex":
		return hex.EncodeToString(data), nil
	case "base64":
		return base64.StdEncoding.EncodeToString(data), nil
	default:
		return "", fmt.Errorf("unsupported format %q, must be one of: utf8, hex, base64", format)
	}
}

// validateFormat validates that the format string is supported.
func validateFormat(format string) error {
	switch strings.ToLower(format) {
	case "utf8", "hex", "base64", "":
		return nil
	default:
		return fmt.Errorf("unsupported format %q, must be one of: utf8, hex, base64", format)
	}
}

// parsePrecondition parses the precondition flags and returns a KVStorePrecondition.
// Returns nil if no precondition is specified.
func parsePrecondition(ifExists, ifMatches string) (*KVStorePrecondition, error) {
	// Check for conflicting preconditions
	if ifExists != "" && ifMatches != "" {
		return nil, fmt.Errorf("cannot specify both --if-exists and --if-matches")
	}

	// No precondition
	if ifExists == "" && ifMatches == "" {
		return nil, nil
	}

	precond := &KVStorePrecondition{}

	// Parse if-exists
	if ifExists != "" {
		exists, err := strconv.ParseBool(ifExists)
		if err != nil {
			return nil, fmt.Errorf("invalid --if-exists value %q, must be 'true' or 'false'", ifExists)
		}
		precond.IfExists = &KVStoreIfExists{Exists: exists}
		return precond, nil
	}

	// Parse if-matches
	if ifMatches != "" {
		// Validate it's a valid hex string (sha256 is 64 hex chars)
		if len(ifMatches) != 64 {
			return nil, fmt.Errorf("invalid --if-matches value, must be a 64-character hex SHA-256 hash")
		}
		if _, err := hex.DecodeString(ifMatches); err != nil {
			return nil, fmt.Errorf("invalid --if-matches value, must be a valid hex string: %v", err)
		}
		precond.IfMatches = &KVStoreIfMatches{SHA256Hash: ifMatches}
		return precond, nil
	}

	return nil, nil
}

// computeSHA256 computes the SHA-256 hash of the given data and returns it as a hex string.
func computeSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// decodeKeys decodes multiple key strings using the specified format.
func decodeKeys(keys []string, format string) ([][]byte, error) {
	if err := validateFormat(format); err != nil {
		return nil, err
	}

	decoded := make([][]byte, len(keys))
	for i, key := range keys {
		decodedKey, err := decodeInput(key, format)
		if err != nil {
			return nil, fmt.Errorf("failed to decode key %q: %v", key, err)
		}
		decoded[i] = decodedKey
	}
	return decoded, nil
}

// KVStoreClient is a client for making kvstore API requests.
type KVStoreClient struct {
	httpClient *http.Client
	baseURL    string
}

// newKVStoreClient creates a new kvstore client configured from the profile.
func newKVStoreClient(p *config.RpkProfile, fs afero.Fs) (*KVStoreClient, error) {
	// Determine base URL from profile configuration
	baseURL := "http://127.0.0.1:8082"
	if len(p.HTTPProxy.Addresses) > 0 {
		baseURL = p.HTTPProxy.Addresses[0]
	}

	// Add scheme if missing
	if !strings.Contains(baseURL, "://") {
		if p.HTTPProxy.TLS != nil {
			baseURL = "https://" + baseURL
		} else {
			baseURL = "http://" + baseURL
		}
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Configure TLS if present in profile
	if p.HTTPProxy.TLS != nil {
		tlsConfig, err := p.HTTPProxy.TLS.Config(fs)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %v", err)
		}
		client.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	return &KVStoreClient{
		httpClient: client,
		baseURL:    baseURL,
	}, nil
}

// Get retrieves keys from a topic partition's kvstore.
func (c *KVStoreClient) Get(ctx context.Context, topic string, partition int32, req KVStoreGetRequest) (*KVStoreGetResponse, error) {
	endpoint := fmt.Sprintf("/kvstore/%s/partition/%d/batch_get", topic, partition)
	var resp KVStoreGetResponse
	if err := c.makeRequest(ctx, endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Write performs put and/or delete operations on a topic partition's kvstore.
func (c *KVStoreClient) Write(ctx context.Context, topic string, partition int32, req KVStoreWriteRequest) (*KVStoreWriteResponse, error) {
	endpoint := fmt.Sprintf("/kvstore/%s/partition/%d/write", topic, partition)
	var resp KVStoreWriteResponse
	if err := c.makeRequest(ctx, endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Scan performs a range scan over keys in a topic partition's kvstore.
func (c *KVStoreClient) Scan(ctx context.Context, topic string, partition int32, req KVStoreScanRequest) (*KVStoreScanResponse, error) {
	endpoint := fmt.Sprintf("/kvstore/%s/partition/%d/scan", topic, partition)
	var resp KVStoreScanResponse
	if err := c.makeRequest(ctx, endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// makeRequest makes an HTTP request to the kvstore API.
func (c *KVStoreClient) makeRequest(ctx context.Context, endpoint string, reqBody any, respBody any) error {
	// Marshal request body
	var bodyReader io.Reader
	if reqBody != nil {
		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %v", err)
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	// Create request
	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, "POST", url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Make request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Unmarshal response if needed
	if respBody != nil && len(body) > 0 {
		if err := json.Unmarshal(body, respBody); err != nil {
			return fmt.Errorf("failed to unmarshal response: %v", err)
		}
	}

	return nil
}

// getTopicPartitionCount retrieves the partition count for a given topic.
func getTopicPartitionCount(ctx context.Context, fs afero.Fs, p *config.RpkProfile, topic string) (int32, error) {
	cl, err := kafka.NewFranzClient(fs, p)
	if err != nil {
		return 0, fmt.Errorf("unable to initialize kafka client: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrMetadataRequest()
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, reqTopic)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, fmt.Errorf("unable to request topic metadata: %v", err)
	}

	if len(resp.Topics) == 0 {
		return 0, fmt.Errorf("topic %q not found", topic)
	}

	topicResp := resp.Topics[0]
	if err := kerr.ErrorForCode(topicResp.ErrorCode); err != nil {
		return 0, fmt.Errorf("error fetching metadata for topic %q: %v", topic, err)
	}

	return int32(len(topicResp.Partitions)), nil
}
