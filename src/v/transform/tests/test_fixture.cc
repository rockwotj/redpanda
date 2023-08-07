#include "transform/tests/test_fixture.h"

#include <boost/test/unit_test.hpp>

namespace transform::testing {
ss::future<> fake_sink::write(ss::chunked_fifo<model::record_batch> batches) {
    for (auto& batch : batches) {
        co_await _batches.push_eventually(std::move(batch));
    }
}
ss::future<model::record_batch> fake_sink::read() {
    return _batches.pop_eventually();
}
cluster::notification_id_type fake_source::register_on_write_notification(
  ss::noncopyable_function<void()> cb) {
    return _subscriptions.register_cb(std::move(cb));
}
void fake_source::unregister_on_write_notification(
  cluster::notification_id_type id) {
    return _subscriptions.unregister_cb(id);
}
ss::future<model::offset> fake_source::load_latest_offset() {
    co_return _latest_offset;
}
ss::future<model::record_batch_reader>
fake_source::read_batch(model::offset offset, ss::abort_source* as) {
    BOOST_CHECK_EQUAL(offset, _latest_offset);
    if (!_batches.empty()) {
        model::record_batch_reader::data_t batches;
        while (!_batches.empty()) {
            batches.push_back(_batches.pop());
        }
        _latest_offset = model::next_offset(batches.back().last_offset());
        co_return model::make_memory_record_batch_reader(std::move(batches));
    }
    auto sub = as->subscribe(
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
      [this](const std::optional<std::exception_ptr>& ex) noexcept {
          _batches.abort(ex.value_or(
            std::make_exception_ptr(ss::abort_requested_exception())));
      });
    auto batch = co_await _batches.pop_eventually();
    _latest_offset = model::next_offset(batch.last_offset());
    sub->unlink();
    co_return model::make_memory_record_batch_reader(std::move(batch));
}
ss::future<> fake_source::push_batch(model::record_batch batch) {
    co_await _batches.push_eventually(std::move(batch));
    if (!_corked) {
        _subscriptions.notify();
    }
}
} // namespace transform::testing
