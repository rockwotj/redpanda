#include "pandawasm/compiler.h"
#include "pandawasm/parser.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <initializer_list>

SEASTAR_THREAD_TEST_CASE(parse_simple_module) {
    // |bytes| contains the binary format for the following module:
    //
    //     (func (export "add") (param i32 i32) (result i32)
    //       get_local 0
    //       get_local 1
    //       i32.add)
    //
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    std::vector<uint8_t> bytes{
      0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01,
      0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07,
      0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00, 0x0a, 0x09, 0x01,
      0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, 0x0b};
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
    iobuf buf;
    buf.append(bytes.data(), bytes.size());
    auto parsed = pandawasm::parse_module(std::move(buf)).get();
    BOOST_CHECK_EQUAL(parsed.functions.size(), 1);
    const auto& add_fn = parsed.functions.front();

    pandawasm::jit_compiler compiler;
    auto jitter = compiler.add_func(add_fn.meta);
    jitter->prologue();
    for (const auto& inst : add_fn.body) {
        std::visit(*jitter.get(), inst);
    }
    jitter->epilogue();
}
