#include "transform/rpc/serde.h"

#include "model/record.h"

#include <seastar/core/chunked_fifo.hh>

namespace transform::rpc {
transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, model::record_batch b)
  : topic(std::move(tp.topic)) {
    ss::chunked_fifo<model::record_batch> batches;
    batches.reserve(1);
    batches.push_back(std::move(b));
    partitions.emplace_back(tp.partition, std::move(batches));
}
transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> batches)
  : topic(std::move(tp.topic)) {
    partitions.reserve(1);
    partitions.emplace_back(tp.partition, std::move(batches));
}
transformed_partition_data transformed_partition_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {partition_id, std::move(shared)};
}
transformed_topic_data transformed_topic_data::share() {
    ss::chunked_fifo<transformed_partition_data> shared;
    shared.reserve(partitions.size());
    for (auto& partition : partitions) {
        shared.push_back(partition.share());
    }
    return {topic, std::move(partitions)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<transformed_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}
} // namespace transform::rpc
