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
} // namespace transform::rpc
