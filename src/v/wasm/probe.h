#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>
namespace wasm {

class probe {
public:
    probe();
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() = default;

    std::unique_ptr<hdr_hist::measurement> auto_transform_measurement() {
        return _transform_latency.auto_measure();
    }
    void transform_complete() { ++_transform_count; }
    void transform_error() { ++_transform_errors; }

private:
    uint64_t _transform_count{0};
    uint64_t _transform_errors{0};
    hdr_hist _transform_latency;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};
} // namespace wasm
