/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_storage_clients/s3_client.h"
#include "cloud_storage_clients/tests/client_pool_builder.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace cloud_storage_clients;
using namespace cloud_storage_clients::tests;

static constexpr size_t test_part_size = 5_MiB;

// Helper to create iobuf with specific size filled with data
static iobuf make_iobuf_of_size(size_t size) {
    iobuf buf;
    while (buf.size_bytes() < size) {
        auto remaining = size - buf.size_bytes();
        auto chunk_size = std::min(remaining, size_t(4096));
        ss::temporary_buffer<char> tmp(chunk_size);
        // Fill with pattern for debugging
        std::fill(tmp.get_write(), tmp.get_write() + tmp.size(), 'A');
        buf.append(std::move(tmp));
    }
    return buf;
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_basic) {
    s3_imposter_fixture imposter;

    // Set up multipart expectations
    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "test-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-123</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "test-key?partNumber=1&uploadId=test-upload-id-123",
       .body = std::nullopt},

      // UploadPart 2
      {.url = "test-key?partNumber=2&uploadId=test-upload-id-123",
       .body = std::nullopt},

      // CompleteMultipartUpload
      {.url = "test-key?uploadId=test-upload-id-123",
       .body = R"xml(<?xml version="1.0"?>
<CompleteMultipartUploadResult>
    <ETag>"final-etag"</ETag>
</CompleteMultipartUploadResult>)xml"},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("test-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size);

    // Write 6 MiB of data (should trigger one part upload immediately)
    auto data = make_iobuf_of_size(6_MiB);
    upload->put(std::move(data)).get();

    // Write another 5 MiB (should trigger second part)
    data = make_iobuf_of_size(5_MiB);
    upload->put(std::move(data)).get();

    // Complete the upload
    upload->complete().get();

    // Verify requests
    auto requests = imposter.get_requests();
    BOOST_CHECK_GE(requests.size(), 4); // Init + 2 Parts + Complete
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_small_file_optimization) {
    s3_imposter_fixture imposter;

    // Set up expectations for regular PUT (not multipart)
    imposter.set_expectations_and_listen({
      {.url = "small-key", .body = std::nullopt},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload with small file
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("small-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size);

    // Write only 3 MiB (less than part_size)
    auto data = make_iobuf_of_size(3_MiB);
    upload->put(std::move(data)).get();

    // Complete - should use regular PUT, not multipart
    upload->complete().get();

    // Verify only one request was made (the regular PUT)
    auto requests = imposter.get_requests();
    BOOST_REQUIRE_EQUAL(requests.size(), 1);
    BOOST_CHECK_EQUAL(requests[0].method, "PUT");
    // Check that URL does not contain multipart indicators
    std::string url_str(requests[0].url);
    BOOST_CHECK(url_str.find("uploads") == std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_abort) {
    s3_imposter_fixture imposter;

    // Set up multipart expectations including abort
    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "abort-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-456</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "abort-key?partNumber=1&uploadId=test-upload-id-456",
       .body = std::nullopt},

      // AbortMultipartUpload (DELETE needs a body to return success)
      {.url = "abort-key?uploadId=test-upload-id-456", .body = ""},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload abort
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("abort-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size);

    // Write data to trigger multipart initialization
    auto data = make_iobuf_of_size(6_MiB);
    upload->put(std::move(data)).get();

    // Abort instead of completing
    upload->abort().get();

    // Verify abort was called
    auto requests = imposter.get_requests();
    BOOST_CHECK_GE(requests.size(), 3); // Init + Part + Abort

    // Check that the last request was DELETE (abort)
    bool found_abort = false;
    for (const auto& req : requests) {
        if (
          req.method == "DELETE"
          && req.url.find("uploadId=test-upload-id-456") != std::string::npos) {
            found_abort = true;
            break;
        }
    }
    BOOST_CHECK(found_abort);
}
