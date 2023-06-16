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

// schemaId is an ID of a schema registered with schema registry
type schemaId uint32

// SchemaType is an enum for the different types of schemas that can be stored in schema registry.
type SchemaType int

const (
	TypeAvro SchemaType = iota
	TypeProtobuf
	TypeJSON
)

// Schema is a schema that can be registered within schema registry
type Schema struct {
	Schema     []byte
	Type       SchemaType
	References []*Schema
}

// SchemaSubject is a schema along with the subject, version and ID of the schema
type SubjectSchema struct {
	Schema

	Subject string
	Version int
	ID      int
}

// SchemaRegistryClient is a client for interacting with the schema registry within Redpanda.
//
// The client provides caching out of the box, which can be configured with options.
type SchemaRegistryClient interface {
	LookupSchemaById(id int) (s *Schema, err error)
}

type (
	options struct {
		// Max size of the cache for ids. Defaults to -1, which means unbounded, 0 disables the cache
		cacheByIdSize int
		// Max size of the cache for (subject,version). Defaults to -1, which means unbounded, 0 disables the cache
		cacheBySubjectVersionSize int
	}
	Option     interface{ apply(*options) }
	optionFunc func(*options)
)

type schemaRegistryClientImpl struct {
	options
	schemaByIdCache map[schemaId]*Schema
}

func (f optionFunc) apply(opts *options) {
	f(opts)
}

//
func WithMaxIdCacheSize(maxSize int) Option {
	return optionFunc(func(o *options) {
		o.cacheByIdSize = maxSize
	})
}

//
func WithMaxSubjectCacheSize(maxSize int) Option {
	return optionFunc(func(o *options) {
		o.cacheBySubjectVersionSize = maxSize
	})
}

// NewClient creates a new SchemaRegistryClient with the specified options applied.
func NewClient(opts ...Option) SchemaRegistryClient {
	o := options{
		cacheByIdSize:             -1,
		cacheBySubjectVersionSize: -1,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}
	return &schemaRegistryClientImpl{
		options:         o,
		schemaByIdCache: make(map[schemaId]*Schema),
	}
}

func (sr *schemaRegistryClientImpl) LookupSchemaById(id int) (s *Schema, err error) {
	cached, ok := sr.schemaByIdCache[schemaId(id)]
	if ok {
		return cached, nil
	}
	var length int32
	errno := getSchemaDefinitionLen(schemaId(id), unsafe.Pointer(&length))
	if errno != 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf := make([]byte, length)
	result := getSchemaDefinition(schemaId(id), unsafe.Pointer(&buf[0]), length)
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
	sr.schemaByIdCache[schemaId(id)] = s
	return s, nil
}

func (sr *schemaRegistryClientImpl) LookupSchemaLatest(schema string) (s *SubjectSchema, err error) {
	return sr.LookupSchemaByVersion(schema, -1)
}
func (sr *schemaRegistryClientImpl) LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error) {
	return nil, nil
}
