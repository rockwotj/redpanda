/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/tx_errc.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>

namespace cluster::tx {

std::string_view format_as(errc err) {
    switch (err) {
    case errc::none:
        return "tx::errc::none";
    case errc::leader_not_found:
        return "tx::errc::leader_not_found";
    case errc::shard_not_found:
        return "tx::errc::shard_not_found";
    case errc::partition_not_found:
        return "tx::errc::partition_not_found";
    case errc::stm_not_found:
        return "tx::errc::stm_not_found";
    case errc::partition_not_exists:
        return "tx::errc::partition_not_exists";
    case errc::pid_not_found:
        return "tx::errc::pid_not_found";
    case errc::timeout:
        return "tx::errc::timeout";
    case errc::conflict:
        return "tx::errc::conflict";
    case errc::fenced:
        return "tx::errc::fenced";
    case errc::stale:
        return "tx::errc::stale";
    case errc::not_coordinator:
        return "tx::errc::not_coordinator";
    case errc::coordinator_not_available:
        return "tx::errc::coordinator_not_available";
    case errc::preparing_rebalance:
        return "tx::errc::preparing_rebalance";
    case errc::rebalance_in_progress:
        return "tx::errc::rebalance_in_progress";
    case errc::coordinator_load_in_progress:
        return "tx::errc::coordinator_load_in_progress";
    case errc::unknown_server_error:
        return "tx::errc::unknown_server_error";
    case errc::request_rejected:
        return "tx::errc::request_rejected";
    case errc::invalid_producer_epoch:
        return "tx::errc::invalid_producer_epoch";
    case errc::invalid_txn_state:
        return "tx::errc::invalid_txn_state";
    case errc::invalid_producer_id_mapping:
        return "tx::errc::invalid_producer_id_mapping";
    case errc::tx_not_found:
        return "tx::errc::tx_not_found";
    case errc::tx_id_not_found:
        return "tx::errc::tx_id_not_found";
    case errc::partition_disabled:
        return "tx::errc::partition_disabled";
    case errc::concurrent_transactions:
        return "tx::errc::concurrent_transactions";
    case errc::invalid_timeout:
        return "tx::errc::invalid_timeout";
    case errc::producer_creation_error:
        return "tx::errc::producer_creation_error";
    case errc::partition_writes_locked:
        return "tx::errc::partition_writes_locked";
    }
    return "tx::errc::unknown";
}

std::ostream& operator<<(std::ostream& o, errc err) {
    return o << format_as(err);
}

std::string errc_category::message(int ec) const {
    return fmt::to_string(errc(ec));
}

} // namespace cluster::tx
