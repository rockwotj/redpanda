#include "transform/rpc/client.h"

#include "cluster/errc.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/constraints.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "rpc/types.h"
#include "transform/rpc/logger.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/smp.hh>

#include <system_error>

namespace transform::rpc {

client::client(
  model::node_id self,
  ss::sharded<cluster::partition_leaders_table>* l,
  ss::sharded<::rpc::connection_cache>* c,
  ss::sharded<local_service>* s)
  : _self(self)
  , _leaders(l)
  , _connections(c)
  , _local_service(s) {}

ss::future<cluster::errc> client::produce(
  model::topic_partition tp,
  ss::chunked_fifo<model::record_batch> batches,
  model::timeout_clock::duration timeout) {
    vlog(log.trace, "producing {} batches to {}", batches.size(), tp);
    // TODO: Batching!
    // TODO: Retries?
    auto leader = _leaders->local().get_leader(
      model::topic_namespace_view(model::kafka_namespace, tp.topic),
      tp.partition);
    if (!leader) {
        co_return cluster::errc::not_leader;
    }

    ss::chunked_fifo<transformed_topic_data> topic_data;
    topic_data.emplace_back(std::move(tp), std::move(batches));

    if (*leader == _self) {
        auto results = co_await _local_service->local().produce(
          std::move(topic_data), timeout);
        vassert(results.size() == 1, "expected only a single response");
        co_return results.front().err;
    }
    produce_request req(std::move(topic_data), timeout);

    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    *leader,
                    timeout,
                    [req = std::move(req), timeout](
                      impl::transform_rpc_client_protocol proto) mutable {
                        return proto.produce(
                          std::move(req),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    });
    if (resp.has_error()) {
        std::error_code ec = resp.assume_error();
        if (ec.category() == cluster::error_category()) {
            co_return static_cast<cluster::errc>(ec.value());
        } else if (ec.category() == raft::error_category()) {
            switch (static_cast<raft::errc>(ec.value())) {
            case raft::errc::not_leader:
            case raft::errc::leadership_transfer_in_progress:
                co_return cluster::errc::not_leader;
            default:
                break;
            }
        }
        co_return cluster::errc::timeout;
    }
    vassert(
      resp.value().data.results.size() == 1, "expected only a single response");
    co_return resp.value().data.results.front().err;
}

} // namespace transform::rpc
