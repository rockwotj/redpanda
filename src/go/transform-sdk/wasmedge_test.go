// go:build wasmedge

//
// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
//

package redpanda

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/rockwotj/redpanda/src/go/sdk/internal/rwbuf"
	"github.com/rockwotj/redpanda/src/go/sdk/internal/transcoding_example/avro"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

var (
	vm         *wasmedge.VM
	conf       *wasmedge.Configure
	wasi       *wasmedge.Module
	rp         *wasmedge.Module
	record     Record
	serialized []byte
	output     = rwbuf.New(0)
)

func putInt64(mem *wasmedge.Memory, param any, v int64) wasmedge.Result {
	b, err := mem.GetData(uint(param.(int32)), 8)
	if err != nil {
		return wasmedge.Result_Fail
	}
	binary.LittleEndian.PutUint64(b, uint64(v))
	return wasmedge.Result_Success
}
func putInt32(mem *wasmedge.Memory, param any, v int32) wasmedge.Result {
	b, err := mem.GetData(uint(param.(int32)), 4)
	if err != nil {
		return wasmedge.Result_Fail
	}
	binary.LittleEndian.PutUint32(b, uint32(v))
	return wasmedge.Result_Success
}
func putInt16(mem *wasmedge.Memory, param any, v int16) wasmedge.Result {
	b, err := mem.GetData(uint(param.(int32)), 4)
	if err != nil {
		return wasmedge.Result_Fail
	}
	binary.LittleEndian.PutUint16(b, uint16(v))
	return wasmedge.Result_Success
}

func readBatchHeaderHostFn(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	mem := callframe.GetMemoryByIndex(0)
	handle := params[0].(int32)
	if handle != 1 {
		return nil, wasmedge.Result_Fail
	}
	// base offset
	putInt64(mem, params[1], 0)
	// record count
	putInt32(mem, params[2], 1)
	// parittion leader epoch
	putInt32(mem, params[3], 0)
	// attrs
	putInt16(mem, params[4], 0)
	// last offset delta
	putInt32(mem, params[5], 1)
	// base timestamp
	putInt64(mem, params[5], 1691458479582)
	// max timestamp
	putInt64(mem, params[6], 1691458479582)
	// producer id
	putInt64(mem, params[7], 0)
	// producer epoch
	putInt16(mem, params[8], 0)
	// base sequence
	putInt32(mem, params[9], 0)
	return []any{0}, wasmedge.Result_Success
}

func readRecordHostFn(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	mem := callframe.GetMemoryByIndex(0)
	handle := params[0].(int32)
	if handle != 1 {
		return nil, wasmedge.Result_Fail
	}
	offset := params[1].(int32)
	length := params[2].(int32)
	b, err := mem.GetData(uint(offset), uint(length))
	if err != nil {
		return nil, wasmedge.Result_Fail
	}
	if int(length) != len(serialized) {
		return nil, wasmedge.Result_Fail
	}
	copy(b, serialized)
	return []any{length}, wasmedge.Result_Success
}
func writeRecordHostFn(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	mem := callframe.GetMemoryByIndex(0)
	offset := params[0].(int32)
	length := params[1].(int32)
	b, err := mem.GetData(uint(offset), uint(length))
	if err != nil {
		return nil, wasmedge.Result_Fail
	}
	_, err = output.Write(b)
	if err != nil {
		return nil, wasmedge.Result_Fail
	}
	return []any{length}, wasmedge.Result_Success
}

func SetupWasmEdge() error {
	record = makeRandomRecord()
	avroStruct, err := makeFakeAvroStruct()
	if err != nil {
		return err
	}
	{
		buf := rwbuf.New(0)
		err = avroStruct.Serialize(buf)
		if err != nil {
			return err
		}
		record.Value = buf.ReadAll()
	}
	buf := rwbuf.New(0)
	record.serialize(buf)
	serialized = buf.ReadAll()
	wasmedge.SetLogErrorLevel()
	conf = wasmedge.NewConfigure()
	vm = wasmedge.NewVMWithConfig(conf)

	wasi = wasmedge.NewWasiModule(
		[]string{"benchmark"},
		[]string{},
		[]string{},
	)
	err = vm.RegisterModule(wasi)
	if err != nil {
		return err
	}

	rp = wasmedge.NewModule("redpanda")
	rbh := wasmedge.NewFunction(wasmedge.NewFunctionType([]wasmedge.ValType{
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
	}, []wasmedge.ValType{
		wasmedge.ValType_I32,
	}), readBatchHeaderHostFn, nil, 0)
	rp.AddFunction("read_batch_header", rbh)
	rr := wasmedge.NewFunction(wasmedge.NewFunctionType([]wasmedge.ValType{
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
	}, []wasmedge.ValType{
		wasmedge.ValType_I32,
	}), readRecordHostFn, nil, 0)
	rp.AddFunction("read_record", rr)
	wr := wasmedge.NewFunction(wasmedge.NewFunctionType([]wasmedge.ValType{
		wasmedge.ValType_I32,
		wasmedge.ValType_I32,
	}, []wasmedge.ValType{
		wasmedge.ValType_I32,
	}), writeRecordHostFn, nil, 0)
	rp.AddFunction("write_record", wr)
	err = vm.RegisterModule(rp)
	if err != nil {
		return err
	}

	vm.LoadWasmFile("./internal/transcoding_example/opt.wasm")
	err = vm.Validate()
	if err != nil {
		return err
	}
	err = vm.Instantiate()
	if err != nil {
		return err
	}
	_, err = vm.Execute("_start")
	return err
}
func makeFakeAvroStruct() (avro.Interop, error) {
	i := avro.Interop{}
	err := gofakeit.Struct(&i)
	i.EnumField = avro.KindB
	i.UnionField.UnionType = avro.UnionBoolDoubleArrayBytesTypeEnumBool
	i.UnionField.Bool = true
	i.RecordField = avro.NewNode()
	i.RecordField.Label = gofakeit.UUID()
	i.RecordField.Children = append(i.RecordField.Children, avro.NewNode())
	i.RecordField.Children = append(i.RecordField.Children, avro.NewNode())
	i.RecordField.Children[0].Label = gofakeit.AchAccount()
	i.RecordField.Children[1].Label = gofakeit.AdjectiveDemonstrative()
	i.RecordField.Children[1].Children = append(i.RecordField.Children[1].Children, avro.NewNode())
	i.RecordField.Children[1].Children[0].Label = gofakeit.AppAuthor()
	return i, err
}

func RunWasmEdge() error {
	results, err := vm.Execute("redpanda_on_record", int32(1), int32(1), int32(len(serialized)), int32(0))
	if err != nil {
		return err
	}
	if len(results) != 1 || results[0].(int32) != 0 {
		return errors.New("bad result")
	}
	output.Reset()
	return nil
}

func Benchmark(b *testing.B) {
	err := SetupWasmEdge()
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Release()
	defer conf.Release()
	for i := 0; i < b.N; i++ {
		err := RunWasmEdge()
		if err != nil {
			b.Fatal(err)
		}
	}
}
