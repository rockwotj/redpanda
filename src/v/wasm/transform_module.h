/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "utils/named_type.h"
#include "wasm/ffi.h"
#include "wasm/wasi.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string_view>

namespace wasm {

struct record_position {
    size_t start_index;
    size_t size;

    int32_t timestamp_delta;
};

struct output_batch {
    // The serialized output records.
    iobuf records;
    // The number of output records we've written.
    int record_count{0};
};

// The data needed during a single transformation of a record_batch
struct transform_context {
    // The input record_batch being transformed.
    const model::record_batch* input;
    // The largest record size for the input batch.
    size_t max_input_record_size{0};
    int32_t latest_record_timestamp_delta;
    // The current record being transformed
    ss::chunked_fifo<record_position> record_positions;
    // The output batches
    ss::chunked_fifo<output_batch> output_batches;
    // This callback is triggered before each record is read
    ss::noncopyable_function<void()> pre_record_callback;
};

struct transform_args {
    const model::record_batch* batch;
    ss::noncopyable_function<void()> pre_record_callback;
};

/**
 * The WASM module for redpanda transform specific host calls.
 *
 * This provides an ABI to WASM guests, as well as the mechanism for
 * guest<->host interactions (such as how we call into a wasm host and when).
 */
class transform_module {
public:
    explicit transform_module(wasi::preview1_module* wasi_module);
    transform_module(const transform_module&) = delete;
    transform_module& operator=(const transform_module&) = delete;
    transform_module(transform_module&&) = delete;
    transform_module& operator=(transform_module&&) = delete;
    ~transform_module() = default;

    static constexpr std::string_view name = "redpanda_transform";

    /**
     * Wait for the transform module to be initialized.
     */
    ss::future<> wait_ready();

    ss::future<ss::chunked_fifo<model::record_batch>> transform(transform_args);

    /**
     * Initializes the module.
     */
    void start();
    /**
     * Stops the module aborting any currently running transform.
     */
    void stop(const std::exception_ptr& ex);

    // Start ABI exports

    void check_abi_version_1();

    ss::future<int32_t> read_batch_header(
      int64_t* base_offset,
      int32_t* record_count,
      int32_t* partition_leader_epoch,
      int16_t* attributes,
      int32_t* last_offset_delta,
      int64_t* base_timestamp,
      int64_t* max_timestamp,
      int64_t* producer_id,
      int16_t* producer_epoch,
      int32_t* base_sequence);

    int32_t read_record(ffi::array<uint8_t>);

    int32_t write_record(ffi::array<uint8_t>);

    // End ABI exports

private:
    struct expected_record_metadata {
        int32_t offset;
        int32_t timestamp;
    };

    struct validation_result {
        bool is_valid;
        bool new_batch;
    };

    validation_result validate_serialized_record(
      iobuf_const_parser parser, expected_record_metadata);

    /*
     * Signal that there is a batch available to be processed via _call_ctx
     * being set.
     *
     * Waits until the batch has been transformed and the results are ready in
     * _call_ctx.
     *
     * This should be called by the outside module to transfer batches into the
     * vm fiber.
     */
    ss::future<> signal_batch_ready();

    /*
     * Signal that a batch has completed processing and wait until another batch
     * is ready to be processed.
     *
     * This should be called by the vm fiber signalling that it's ready to wait
     * for another batch to transform.
     */
    ss::future<> signal_batch_complete();

    wasi::preview1_module* _wasi_module;

    ss::condition_variable _input_batch_ready;
    ss::condition_variable _waiting_for_next_input_batch;
    // We expect to be able to reuse VMs (and share them) so instead of using
    // `broken` for the condition variables we explicitly track a seperate
    // exception and check it whenever we wait/signal for a batch.
    std::exception_ptr _abort_ex = nullptr;

    std::optional<transform_context> _call_ctx;
};
} // namespace wasm
