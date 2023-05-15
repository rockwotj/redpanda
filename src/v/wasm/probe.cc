#include "probe.h"

#include "prometheus/prometheus_sanitize.h"

namespace wasm {

probe::probe() {
    namespace sm = ss::metrics;

    std::vector<sm::label_instance> labels{
      sm::label("latency_metric")("microseconds")};
    auto aggregate_labels = std::vector<sm::label>{sm::shard_label};
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("redpanda:wasm"),
      {
        sm::make_histogram(
          "latency_us",
          sm::description("Wasm Latency"),
          labels,
          [this] { return _transform_latency.seastar_histogram_logform(); })
          .aggregate(aggregate_labels),
        sm::make_counter(
          "count",
          [this] { return _transform_count; },
          sm::description("Wasm transforms total count"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "errors",
          [this] { return _transform_errors; },
          sm::description("Wasm errors"),
          labels)
          .aggregate(aggregate_labels),
      });
}

} // namespace wasm
