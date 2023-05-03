#pragma once

using uint32_t = unsigned int;
using int32_t = int;
using uint8_t = unsigned char;
using int64_t = long;
using uint64_t = unsigned long;

#ifndef __wasm32__
#error "abi.h is only supported on wasi targets"
#endif

#define WASM_IMPORT(name) \
    __attribute__((__import_module__("redpanda"), __import_name__(#name))) extern "C"

#define WASM_EXPORT(name) __attribute__((visibility("default"), __export_name__(#name))) extern "C"

// These functions are available under the user's WASM module and they are
// expected to implement these.
//
// A WASM ABI is very low level, and can essentially use the following data
// types:
//
// int8_t*
// uint8_t*
//
// int32_t
// int32_t*
// uint32_t
// uint32_t*
//
// int64_t
// int64_t*
// uint64_t
// uint64_t*
//
// float
// float*
// double
// double*
//
// Due to memory constraints within Redpanda, we support a streaming values in
// and out of the WASM VM.

// Return the version that the ABI supports (maybe we can remove this if wasm
// engines allow for reading a global - or we can make this a custom section in
// the WASM binary file, but reading that may be annoying).
WASM_EXPORT(redpanda_abi_version) uint32_t redpanda_abi_version();

// Return 0 on success and negative on error (TODO: Define error codes)
using EventErrorCode = int32_t;

using InputRecordHandle = int32_t;

// The entrypoint to a record, when this is called the current record will be
// available from the read methods
WASM_EXPORT(redpanda_on_record) EventErrorCode redpanda_on_record(InputRecordHandle handle);

// All these functions are available under a WASM module called "redpanda"
// These will be provided by the redpanda host process.

// === START: Reading the current record functionality ===

// Return result from a read
// - negative means error (TODO: Define error codes)
// - 0 means eof
// - positive means that much was read from the value
using ReadResult = int32_t;

// Read the key and return how much was written.
//
// TODO: Do we need to distinguish bettween no key and empty key? We can
// probably do that via an error code
WASM_IMPORT(read_key) ReadResult read_key(InputRecordHandle handle, uint8_t* data, uint32_t len);

// Read the value and return how much was written.
//
// If this value was compressed, it will be decompressed when being read.
WASM_IMPORT(read_value) ReadResult read_value(InputRecordHandle handle, uint8_t* data, uint32_t len);

// Return the number of headers in the input record
WASM_IMPORT(num_headers) int32_t num_headers(InputRecordHandle handle);

WASM_IMPORT(find_header_by_key) int32_t find_header_by_key(
  InputRecordHandle handle, uint8_t* key_data, uint32_t key_len);

// Return how a big a header is.
WASM_IMPORT(get_header_key_length) ReadResult get_header_key_length(InputRecordHandle handle, int32_t index);
// Return how a big an individual header is.
WASM_IMPORT(get_header_value_length) ReadResult get_header_value_length(InputRecordHandle handle, int32_t index);

// Read a key's name. An error will be return if the full header cannot fit into
// |key_data|.
WASM_IMPORT(get_header_key) ReadResult get_header_key(
  InputRecordHandle handle, int32_t index, uint8_t* key_data, uint32_t key_len);

// Read header's value into |value_data|. An error will be returned if the full
// header cannot fit into |value_data|.
WASM_IMPORT(get_header_value) ReadResult get_header_value(
  InputRecordHandle handle,
  int32_t index,
  uint8_t* value_data,
  uint32_t value_len);

// The offset of the current record
WASM_IMPORT(offset) uint64_t offset(InputRecordHandle handle);

// Return the timestamp of the current record
WASM_IMPORT(timestamp) uint64_t timestamp(InputRecordHandle handle);

// TODO: Expose more infromation about the current record, like leader_epoch,
// etc.

// === START: Emitting records functionality ===

using OutputRecordHandle = int32_t;

// If you want to write multiple key value pairs in a batch, this will flush the
// current record so a new one can be written.
WASM_IMPORT(create_output_record) OutputRecordHandle create_output_record();

// Write the output record's key and return if the was an error
//
// Return result:
// - negative means error (TODO: Define error codes)
// - anything else means success (we never partially write)
WASM_IMPORT(write_key) int32_t write_key(OutputRecordHandle handle, uint8_t* data, uint32_t len);

// Write the output record's value and return if the was an error
//
// Return result:
// - negative means error (TODO: Define error codes)
// - anything else means success (we never partially write)
WASM_IMPORT(write_value) int32_t write_value(OutputRecordHandle handle, uint8_t* data, uint32_t len);

// Add header to the output record
WASM_IMPORT(append_header) int32_t append_header(
  OutputRecordHandle handle,
  uint8_t* key_data,
  uint32_t key_len,
  uint8_t* value_data,
  uint32_t value_len);

// TODO: Should allow for compression when writing the batch? Should we allow
// for setting timestamps?

// === END: Emitting records functionality ===

// TODO: Support reading from schema registry
// TODO: Network functionality (Do we just use raw sockets or http?)
// TODO: Explicitly layout the WASI support we'll want here
