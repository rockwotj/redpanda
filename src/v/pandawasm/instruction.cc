// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "instruction.h"

namespace pandawasm {

void value_stack::push(value v) { _underlying.push_back(v); }

value value_stack::pop() {
    value v = _underlying.back();
    _underlying.pop_back();
    return v;
}

void frame::set(uint32_t idx, value v) { _underlying[idx] = v; }
value frame::get(uint32_t idx) const { return _underlying[idx]; }

} // namespace pandawasm
