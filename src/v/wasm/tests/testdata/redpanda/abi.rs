/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

type EventErrorCode = i32;
type InputRecordHandle = i32;
type ReadResult = i32;
type OutputRecordHandle = i32;

#[no_mangle]
extern "C" fn redpanda_abi_version() -> u32 {
    return 1;
}

#[no_mangle]
extern "C" fn redpanda_on_record(handle: InputRecordHandle) -> EventErrorCode {
    return 1;
}

#[link(wasm_import_module = "redpanda")]
extern "C" {
    fn read_key(handle: InputRecordHandle, data: *mut u8, len: u32) -> ReadResult;
    fn read_value(handle: InputRecordHandle, data: *mut u8, len: u32) -> ReadResult;
    fn num_headers(handle: InputRecordHandle) -> i32;
    fn find_header_by_key(handle: InputRecordHandle, key_data: *mut u8, key_len: u32) -> i32;
    fn get_header_key_length(handle: InputRecordHandle, index: i32) -> ReadResult;
    fn get_header_value_length(handle: InputRecordHandle, index: i32) -> ReadResult;
    fn get_header_key(
        handle: InputRecordHandle,
        index: i32,
        key_data: *mut u8,
        key_len: u32,
    ) -> ReadResult;
    fn get_header_value(
        handle: InputRecordHandle,
        index: i32,
        value_data: *mut u8,
        value_len: u32,
    ) -> ReadResult;
    fn offset(handle: InputRecordHandle) -> u64;
    fn timestamp(handle: InputRecordHandle) -> u64;
    fn create_output_record() -> OutputRecordHandle;
    fn write_key(handle: OutputRecordHandle, data: *mut u8, len: u32) -> i32;
    fn write_value(handle: OutputRecordHandle, data: *mut u8, len: u32) -> i32;
    fn append_header(
        handle: OutputRecordHandle,
        key_data: *mut u8,
        key_len: u32,
        value_data: *mut u8,
        value_len: u32,
    ) -> i32;
}

type HttpResponseHandle = i32;
const HTTP_METHOD_GET: http_method = 1;
const HTTP_METHOD_POST: http_method = 2;
type http_method = u32;

#[link(wasm_import_module = "redpanda")]
extern "C" {
    fn http_fetch(
        method: http_method,
        url: *const u8,
        url_len: u32,
        headers: *const u8,
        headers_len: u32,
        body: *const u8,
        body_len: u32,
    ) -> HttpResponseHandle;
    fn read_http_resp_body(handle: HttpResponseHandle, buf: *mut u8, buf_len: u32) -> ReadResult;
}
