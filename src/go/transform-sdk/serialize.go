// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redpanda

import (
	"github.com/redpanda-data/redpanda/src/go/transform-sdk/internal/rwbuf"
)

// Reusable slice of record headers
var incomingRecordHeaders []RecordHeader = nil

func readKV(offsets []int32, b *rwbuf.RWBuf) ([]byte, []byte, []int32, error) {
	k, err := b.ReadSlice(int(offsets[0]))
	if err != nil {
		return nil, nil, nil, err
	}
	v, err := b.ReadSlice(int(offsets[1]))
	if err != nil {
		return nil, nil, nil, err
	}
	return k, v, offsets[2:], err
}

func (r *Record) deserializePayload(offsets []int32, b *rwbuf.RWBuf) error {
	k, v, offsets, err := readKV(offsets, b)
	if err != nil {
		return err
	}
	r.Key = k
	r.Value = v
	incomingRecordHeaders = incomingRecordHeaders[0:0]
	for len(offsets) > 0 {
		k, v, offsets, err = readKV(offsets, b)
		if err != nil {
			return err
		}
		incomingRecordHeaders = append(incomingRecordHeaders, RecordHeader{
			Key:   k,
			Value: v,
		})
	}
	r.Headers = incomingRecordHeaders
	return nil
}

// Serialize this output record's payload (key, value, headers) into the buffer
func (r Record) serializePayload(offsets []int32, b *rwbuf.RWBuf) {
	offsets[0] = int32(len(r.Key))
	_, _ = b.Write(r.Key)
	offsets[1] = int32(len(r.Value))
	_, _ = b.Write(r.Value)
	for i, h := range r.Headers {
		offsets[(i*2)+2] = int32(len(h.Key))
		_, _ = b.Write(h.Key)
		offsets[(i*2)+3] = int32(len(h.Value))
		_, _ = b.Write(h.Value)
	}
}
