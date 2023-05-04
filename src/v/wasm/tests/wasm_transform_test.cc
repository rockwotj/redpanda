
#include "model/tests/random_batch.h"
#include "seastarx.h"
#include "wasm/wasm.h"
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>
#include <seastar/testing/thread_test_case.hh>
#include <boost/concept_check.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <wasmedge/enum_types.h>

SEASTAR_THREAD_TEST_CASE(test_wasm_transforms_work) {
  auto wasm_file = ss::util::read_entire_file_contiguous("identity_transform.wasm").get0();
  auto engine = wasm::make_wasm_engine(std::move(wasm_file)).assume_value();
  auto batch = model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      });
  auto result_batches = engine->transform(batch.copy()).get0();
  BOOST_CHECK_EQUAL(result_batches.size(), 1);
  BOOST_CHECK_EQUAL(result_batches[0].copy_records(), batch.copy_records());
  BOOST_CHECK_EQUAL(result_batches[0], batch);
}

