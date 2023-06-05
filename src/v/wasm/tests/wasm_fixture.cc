#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/reactor.hh>

wasm_test_fixture::wasm_test_fixture()
  : _service(&_worker) {
    _worker.start().get();
    // wasmtime uses SIGILL to handle traps, by default these fail tests, so we
    // register this handler as a noop.
    // Additionally, this signal handler is registered globally once, so only
    // one test case will be able to setup seastar at a time.
    seastar::engine().handle_signal(SIGILL, [] {});
}
wasm_test_fixture::~wasm_test_fixture() { _worker.stop().get(); }

std::unique_ptr<wasm::engine>
wasm_test_fixture::load_engine(const std::string& path) {
    auto wasm_file = ss::util::read_entire_file_contiguous(path).get0();
    return _service.make_wasm_engine(path, std::move(wasm_file)).get0();
}

wasm::probe* wasm_test_fixture::probe() const { return _service.get_probe(); }

model::record_batch wasm_test_fixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
    });
}
