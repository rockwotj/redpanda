/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/lookup_value_for_key.h"

namespace kafka {

using join_group_handler = two_phase_handler<lookup_value_for_key_api, 0, 6>;

} // namespace kafka
