#include "wasm/tests/wasm_fixture.h"

#include "model/record_batch_reader.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/reactor.hh>

wasm_test_fixture::wasm_test_fixture()
  : _service(&_worker)
  , _meta(
      "test_wasm_transform",
      model::random_topic_namespace(),
      model::random_topic_namespace()) {
    _worker.start().get();
    // wasmtime uses SIGILL to handle traps, by default these fail tests, so we
    // register this handler as a noop.
    // Additionally, this signal handler is registered globally once, so only
    // one test case will be able to setup seastar at a time.
    seastar::engine().handle_signal(SIGILL, [] {});
}
wasm_test_fixture::~wasm_test_fixture() { _worker.stop().get(); }

void wasm_test_fixture::load_wasm(const std::string& path) {
    auto wasm_file = ss::util::read_entire_file_contiguous(path).get0();
    _service.deploy_transform(_meta, wasm_file).get();
}

ss::circular_buffer<model::record_batch>
wasm_test_fixture::transform(const model::record_batch& batch) {
    auto reader = _service.wrap_batch_reader(
      _meta.input, model::make_memory_record_batch_reader(batch.copy()));
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .get();
}
model::record_batch wasm_test_fixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
    });
}
