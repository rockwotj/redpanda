// Copyright 2024 Redpanda Data, Inc.
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

#include <redpanda/transform_sdk.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <string>

#ifdef __wasi__
#define WASM_IMPORT(mod, name)                                                 \
    __attribute__((import_module(#mod), import_name(#name)))
#else
#define WASM_IMPORT(mod, name)
#endif

extern "C" {
WASM_IMPORT(redpanda_ai, compute_embeddings)
int32_t redpanda_ai_compute_embeddings(
  const uint8_t* prompt_data,
  size_t prompt_len,
  const float* generated_output,
  size_t generated_output_len);
}

namespace rp {
using namespace redpanda; // NOLINT
} // namespace rp

static constexpr size_t max_embedding_size = 2048;
std::vector<float> needle_embeddings; // NOLINT

float threshold = 0.45; // NOLINT

float embd_similarity_cos(std::span<const float> embd) {
    float sum = 0.0;
    float sum1 = 0.0;
    float sum2 = 0.0;

    size_t min = std::min(embd.size(), needle_embeddings.size());
    for (size_t i = 0; i < min; ++i) {
        sum += embd[i] * needle_embeddings[i];
        sum1 += needle_embeddings[i] * needle_embeddings[i];
        sum2 += embd[i] * embd[i];
    }
    return sum / (std::sqrt(sum1) * std::sqrt(sum2));
}

std::vector<rp::bytes_view> split_sentences(rp::bytes_view text) {
    std::vector<rp::bytes_view> result;
    while (!text.empty()) {
        const auto* it = std::find(text.begin(), text.end(), '.');
        if (it == text.end()) {
            break;
        }
        size_t len = std::distance(text.begin(), it);
        if (len > 3) {
            result.push_back(text.subview(0, len));
        }
        text = text.subview(len + 1);
    }
    if (!text.empty()) {
        result.push_back(text);
    }
    return result;
}

std::error_code
embedding_filter(const rp::write_event& event, rp::record_writer* writer) {
    rp::bytes_view value = event.record.value.value_or(rp::bytes_view());
    std::array<float, max_embedding_size> output; // NOLINT
    for (rp::bytes_view sentence : split_sentences(value)) {
        size_t generated_amount = redpanda_ai_compute_embeddings(
          sentence.data(), sentence.size(), output.data(), output.size());
        auto embeddings = std::span<const float>{
          output.data(), generated_amount};
        if (embd_similarity_cos(embeddings) >= threshold) {
            return writer->write(event.record);
        }
    }
    return {};
}

int main() {
    const char* q = std::getenv("QUESTION"); // NOLINT
    assert(q != nullptr);
    const char* t = std::getenv("THRESHOLD"); // NOLINT
    if (t != nullptr) {
        threshold = std::stof(t);
    }
    std::string_view needle = q;
    needle_embeddings.resize(max_embedding_size);
    size_t result_size = redpanda_ai_compute_embeddings(
      reinterpret_cast<const uint8_t*>(needle.data()), // NOLINT
      needle.size(),
      needle_embeddings.data(),
      needle_embeddings.size());
    needle_embeddings.resize(result_size);
    rp::on_record_written(embedding_filter);
}
