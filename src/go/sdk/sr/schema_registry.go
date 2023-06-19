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
package sr

import (
	"encoding/binary"
	"errors"
	"strconv"
	"unsafe"

	"github.com/rockwotj/redpanda/src/go/sdk/internal/rwbuf"
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

// SchemaReference is a way for a one schema to reference another. The details
// for how referencing is done are type specific; for example, JSON objects
// that use the key "$ref" can refer to another schema via URL. For more details
// on references, see the following link:
//
//	https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#schema-references
//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html
type Reference struct {
	Name    string
	Subject string
	Version int
}

// Schema is a schema that can be registered within schema registry
type Schema struct {
	Schema     string
	Type       SchemaType
	References []Reference
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
	// LookupSchemaById looks up a schema via it's global ID.
	LookupSchemaById(id int) (s *Schema, err error)
	// LookupSchemaByVersion looks up a schema via a subject for a specific version.
	//
	// Use version -1 to get the latest version.
	LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error)
}

type (
	clientOpts struct {
		disableCaches bool
	}
	// ClientOpt is an option to configure a SchemaRegistryClient
	ClientOpt     interface{ apply(*clientOpts) }
	clientOptFunc func(*clientOpts)
)

func (f clientOptFunc) apply(opts *clientOpts) {
	f(opts)
}

type subjectVersion struct {
	subject string
	version int
}

type (
	clientImpl struct {
	}
	cachingClientImpl struct {
		underlying                  SchemaRegistryClient
		schemaByIdCache             map[schemaId]*Schema
		schemaBySubjectVersionCache map[subjectVersion]*SubjectSchema
	}
)

// WithCachesDisabled disables any caching done by the client
func WithCachesDisabled() ClientOpt {
	return clientOptFunc(func(o *clientOpts) {
		o.disableCaches = true
	})
}

// NewClient creates a new SchemaRegistryClient with the specified options applied.
func NewClient(opts ...ClientOpt) (c SchemaRegistryClient) {
	o := clientOpts{
		disableCaches: false,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}
	c = &clientImpl{}
	if o.disableCaches {
		return c
	}
	c = &cachingClientImpl{
		underlying:                  c,
		schemaByIdCache:             make(map[schemaId]*Schema),
		schemaBySubjectVersionCache: make(map[subjectVersion]*SubjectSchema),
	}
	return c
}

func (sr *cachingClientImpl) LookupSchemaById(id int) (s *Schema, err error) {
	cached, ok := sr.schemaByIdCache[schemaId(id)]
	if ok {
		return cached, nil
	}
	s, err = sr.underlying.LookupSchemaById(id)
	if err != nil {
		sr.schemaByIdCache[schemaId(id)] = s
	}
	return
}
func (sr *clientImpl) LookupSchemaById(id int) (*Schema, error) {
	var length int32
	errno := getSchemaDefinitionLen(schemaId(id), unsafe.Pointer(&length))
	if errno != 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf := rwbuf.New(int(length))
	result := getSchemaDefinition(
		schemaId(id),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()),
	)
	if result < 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf.AdvanceWriter(int(result))
	schema, err := decodeSchemaDef(buf)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

func (sr *cachingClientImpl) LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error) {
	cached, ok := sr.schemaBySubjectVersionCache[subjectVersion{subject, version}]
	if ok {
		return cached, nil
	}
	s, err = sr.underlying.LookupSchemaByVersion(subject, version)
	if err != nil {
		sr.schemaBySubjectVersionCache[subjectVersion{subject, version}] = s
	}
	return
}

// LookupSchemaById looks up a schema via a subject for a specific version.
func (sr *clientImpl) LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error) {
	var length int32
	errno := getSchemaSubjectLen(
		unsafe.Pointer(unsafe.StringData(subject)),
		int32(len(subject)),
		int32(version),
		unsafe.Pointer(&length),
	)
	if errno != 0 {
		return nil, errors.New("unable to find a schema " + subject + " with version " + strconv.Itoa(int(version)))
	}
	buf := rwbuf.New(int(length))
	result := getSchemaSubject(
		unsafe.Pointer(unsafe.StringData(subject)),
		int32(len(subject)),
		int32(version),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()),
	)
	if result < 0 {
		return nil, errors.New("unable to find a schema " + subject + " and version " + strconv.Itoa(version))
	}
	buf.AdvanceWriter(int(result))
	schema, err := decodeSchema(subject, buf)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

func decodeSchema(subject string, buf *rwbuf.RWBuf) (s SubjectSchema, err error) {
	s.Subject = subject
	id, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.ID = int(id)
	v, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.Version = int(v)
	s.Schema, err = decodeSchemaDef(buf)
	return
}

func decodeSchemaDef(buf *rwbuf.RWBuf) (s Schema, err error) {
	t, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.Type = SchemaType(t)
	s.Schema, err = buf.ReadSizedStringCopy()
	if err != nil {
		return s, err
	}
	rc, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.References = make([]Reference, rc)
	for i := int64(0); i < rc; i++ {
		s.References[i].Name, err = buf.ReadSizedStringCopy()
		if err != nil {
			return
		}
		s.References[i].Name, err = buf.ReadSizedStringCopy()
		if err != nil {
			return
		}
		v, err := binary.ReadVarint(buf)
		if err != nil {
			return s, err
		}
		s.References[i].Version = int(v)
	}
	return
}
