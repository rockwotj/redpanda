// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newLookupCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lookup TOPIC KEY",
		Short: "Lookup record values from a topic by key",
		Long:  helpLookup,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			topic := args[0]
			var keys [][]byte
			for _, keyStr := range args[1:] {
				keys = append(keys, []byte(keyStr))
			}
			values := make([][]byte, len(keys))
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			cl, err := kafka.NewFranzClient(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			adm := kadm.NewClient(cl)
			md, err := adm.Metadata(cmd.Context(), topic)
			out.MaybeDie(err, "unable to request metadata: %v", err)
			err = md.Topics.Error()
			out.MaybeDie(err, "unable to request topic metadata: %v", err)
			for _, p := range md.Topics[topic].Partitions {
				out.MaybeDie(p.Err, "unable to load partition %d: %v", p.Partition, p.Err)
				if p.Leader == -1 {
					out.Die("unable to determine leader for partition %d", p.Partition)
				}
				req := &lookupValueRequest{
					Topics: []lookupValueForTopicRequest{{
						Topic: topic,
						Partitions: []lookupValueForPartitionRequest{{
							PartitionIndex: p.Partition,
							Keys:           keys,
						}},
					}},
				}
				rawResp, err := cl.Request(cmd.Context(), req)
				out.MaybeDie(err, "unable to make request to partition %d: %v", p.Partition, err)
				resp := rawResp.(*lookupValueResponse)
				err = kerr.ErrorForCode(resp.ErrorCode)
				out.MaybeDie(err, "unable to fetch keys for partition %d: %v", p.Partition, err)
				if len(resp.Responses) != 1 {
					out.Die("unexpected number of responses on partition %d: %v", p.Partition, len(resp.Responses))
				}
				topicResp := resp.Responses[0]
				if topicResp.Topic != topic || len(topicResp.Partitions) != 1 || topicResp.Partitions[0].PartitionIndex != p.Partition {
					var partitions []int32
					for _, p := range topicResp.Partitions {
						partitions = append(partitions, p.PartitionIndex)
					}
					out.Die("unexpected responses on partition %d: topic=%v, partitions=%v", p.Partition, topicResp.Topic, partitions)
				}
				partitionResp := topicResp.Partitions[0]
				err = kerr.ErrorForCode(partitionResp.ErrorCode)
				out.MaybeDie(err, "unable to fetch keys for partition %d: %v", p.Partition, err)
				allFound := true
				for i, v := range partitionResp.Values {
					if values[i] == nil {
						values[i] = v.Data
					}
					allFound = allFound && values[i] != nil
				}
				if allFound {
					break
				}
			}
			for i := range keys {
				os.Stdout.Write(values[i])
				os.Stdout.Write(newline)
			}
		},
	}
	return cmd
}

type (
	lookupValueRequest struct {
		Version int16

		Topics []lookupValueForTopicRequest
	}

	lookupValueForTopicRequest struct {
		Topic      string
		Partitions []lookupValueForPartitionRequest
	}

	lookupValueForPartitionRequest struct {
		PartitionIndex int32
		Keys           [][]byte
	}

	lookupValueResponse struct {
		Version int16

		ThrottleTimeMs int32
		ErrorCode      int16
		Responses      []lookupValueForTopicResponse
	}

	lookupValueForTopicResponse struct {
		Topic      string
		Partitions []lookupValueForPartitionResponse
	}

	lookupValueForPartitionResponse struct {
		PartitionIndex int32
		ErrorCode      int16
		Values         []lookupValueData
	}

	lookupValueData struct {
		Data []byte
	}
)

var (
	_ kmsg.Request             = (*lookupValueRequest)(nil)
	_ kmsg.Response            = (*lookupValueResponse)(nil)
	_ kmsg.ThrottleResponse    = (*lookupValueResponse)(nil)
	_ kmsg.SetThrottleResponse = (*lookupValueResponse)(nil)
)

func (l *lookupValueRequest) GetVersion() int16  { return l.Version }
func (l *lookupValueRequest) SetVersion(v int16) { l.Version = v }
func (l *lookupValueRequest) IsFlexible() bool   { return true }
func (l *lookupValueRequest) Key() int16         { return 15000 }
func (l *lookupValueRequest) MaxVersion() int16  { return 0 }
func (l *lookupValueRequest) Default()           {}
func (l *lookupValueRequest) ResponseKind() kmsg.Response {
	return &lookupValueResponse{Version: l.Version}
}

// ReadFrom implements kmsg.Request.
func (l *lookupValueRequest) ReadFrom(src []byte) error {
	b := kbin.Reader{Src: src}
	topicLen := b.CompactArrayLen()
	if topicLen > 0 {
		l.Topics = make([]lookupValueForTopicRequest, topicLen)
	}
	for i := int32(0); i < topicLen; i++ {
		t := &l.Topics[i]
		t.Topic = b.CompactString()
		partitionLen := b.CompactArrayLen()
		if partitionLen > 0 {
			t.Partitions = make([]lookupValueForPartitionRequest, partitionLen)
		}
		for j := int32(0); j < partitionLen; j++ {
			p := &t.Partitions[j]
			p.PartitionIndex = b.Int32()
			keysLen := b.CompactArrayLen()
			if keysLen > 0 {
				p.Keys = make([][]byte, keysLen)
			}
			for k := int32(0); k < keysLen; k++ {
				p.Keys[k] = b.CompactBytes()
			}
		}
	}
	return b.Complete()
}

// AppendTo implements kmsg.Request.
func (l *lookupValueRequest) AppendTo(dst []byte) []byte {
	dst = kbin.AppendCompactArrayLen(dst, len(l.Topics))
	for _, topic := range l.Topics {
		dst = kbin.AppendCompactString(dst, topic.Topic)
		dst = kbin.AppendCompactArrayLen(dst, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			dst = kbin.AppendInt32(dst, partition.PartitionIndex)
			dst = kbin.AppendCompactArrayLen(dst, len(partition.Keys))
			for _, key := range partition.Keys {
				dst = kbin.AppendCompactBytes(dst, key)
			}
		}
	}
	return dst
}

func (l *lookupValueResponse) GetVersion() int16  { return 0 }
func (l *lookupValueResponse) SetVersion(v int16) { l.Version = v }
func (l *lookupValueResponse) IsFlexible() bool   { return true }
func (l *lookupValueResponse) Key() int16         { return 15000 }
func (l *lookupValueResponse) MaxVersion() int16  { return 0 }
func (l *lookupValueResponse) Default()           {}
func (l *lookupValueResponse) RequestKind() kmsg.Request {
	return &lookupValueRequest{Version: l.Version}
}
func (l *lookupValueResponse) Throttle() (int32, bool) { return l.ThrottleTimeMs, true }
func (l *lookupValueResponse) SetThrottle(v int32)     { l.ThrottleTimeMs = v }

func (l *lookupValueResponse) ReadFrom(src []byte) error {
	b := kbin.Reader{Src: src}
	topicLen := b.CompactArrayLen()
	if topicLen > 0 {
		l.Responses = make([]lookupValueForTopicResponse, topicLen)
	}
	for i := range topicLen {
		t := &l.Responses[i]
		t.Topic = b.CompactString()
		partitionLen := b.CompactArrayLen()
		if partitionLen > 0 {
			t.Partitions = make([]lookupValueForPartitionResponse, partitionLen)
		}
		for j := range partitionLen {
			p := &t.Partitions[j]
			p.PartitionIndex = b.Int32()
			p.ErrorCode = b.Int16()
			valuesLen := b.CompactArrayLen()
			if valuesLen > 0 {
				p.Values = make([]lookupValueData, valuesLen)
			}
			for k := range valuesLen {
				p.Values[k].Data = b.NullableBytes()
			}
		}
	}
	return b.Complete()
}

// AppendTo implements kmsg.Response.
func (l *lookupValueResponse) AppendTo(dst []byte) []byte {
	dst = kbin.AppendCompactArrayLen(dst, len(l.Responses))
	for _, topic := range l.Responses {
		dst = kbin.AppendCompactString(dst, topic.Topic)
		dst = kbin.AppendCompactArrayLen(dst, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			dst = kbin.AppendInt32(dst, partition.PartitionIndex)
			dst = kbin.AppendInt16(dst, partition.ErrorCode)
			dst = kbin.AppendCompactArrayLen(dst, len(partition.Values))
			for _, value := range partition.Values {
				dst = kbin.AppendCompactNullableBytes(dst, value.Data)
			}
		}
	}
	return dst
}

const helpLookup = `Lookup values in a topic.

This command uses Redpanda key indexes to lookup a key's value.

`
