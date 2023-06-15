// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package sr

import (
	"errors"
	"strconv"
	"unsafe"
)

type SchemaId uint32

// SchemaType is an enum for the different types of schemas that can be stored in schema registry.
type SchemaType int

const (
	TypeAvro SchemaType = iota
	TypeProtobuf
	TypeJSON
)

type Schema struct {
	Schema     []byte
	Type       SchemaType
	References []*Schema
}

type SchemaRegistryClient interface {
	LookupSchemaById(id SchemaId) (s *Schema, err error)
}

type schemaRegistryClient struct {
	schemaByIdCache map[SchemaId]*Schema
}

func NewClient() SchemaRegistryClient {
	return &schemaRegistryClient{
		schemaByIdCache: make(map[SchemaId]*Schema),
	}
}

func (sr *schemaRegistryClient) LookupSchemaById(id SchemaId) (s *Schema, err error) {
	cached, ok := sr.schemaByIdCache[id]
	if ok {
		return cached, nil
	}
	var length int32
	errno := getSchemaDefinitionLen(SchemaId(id), unsafe.Pointer(&length))
	if errno != 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf := make([]byte, length)
	result := getSchemaDefinition(SchemaId(id), unsafe.Pointer(&buf[0]), length)
	var t SchemaType
	switch result {
	case 1:
		t = TypeAvro
	case 2:
		t = TypeProtobuf
	case 3:
		t = TypeJSON
	default:
		if result < 0 {
			err = errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
		} else {
			err = errors.New("unknown schema type")
		}
		return
	}
	s = &Schema{
		Schema:     buf,
		Type:       t,
		References: []*Schema{},
	}
	sr.schemaByIdCache[id] = s
	return s, nil
}
