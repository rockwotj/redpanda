// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"
#include "syschecks/syschecks.h"
#include "tcmalloc/malloc_extension.h"

namespace debug {
application* app;
}

int main(int argc, char** argv, char** /*env*/) {
    // must be the first thing called
    syschecks::initialize_intrinsics();
    // 512KiB is golang's default sampling rate.
    constexpr size_t sample_rate = size_t(512) * 1024 * 1024;
    tcmalloc::MallocExtension::SetProfileSamplingRate(sample_rate);
    void* ptr = ::operator new(sample_rate * 2);
    std::cout << "allocated: " << ptr << "ownership: "
              << int(tcmalloc::MallocExtension::GetOwnership(ptr)) << "\n";
    ::operator delete(ptr);
    application app;
    debug::app = &app;
    return app.run(argc, argv);
}
