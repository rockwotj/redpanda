#include "transform/rpc/client.h"

#include "cluster/errc.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/constraints.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"
#include "transform/rpc/logger.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer_fixed_capacity.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <boost/fusion/sequence/intrinsic/back.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace transform::rpc {

namespace {
constexpr auto timeout = std::chrono::seconds(1);

cluster::errc map_errc(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return static_cast<cluster::errc>(ec.value());
    } else if (ec.category() == raft::error_category()) {
        auto raft_ec = static_cast<raft::errc>(ec.value());
        switch (raft_ec) {
        case raft::errc::not_leader:
        case raft::errc::leadership_transfer_in_progress:
            return cluster::errc::not_leader;
        default:
            vlog(log.error, "unexpected transform produce error: {}", raft_ec);
            break;
        }
    } else if (ec.category() == ::rpc::error_category()) {
        auto rpc_ec = static_cast<::rpc::errc>(ec.value());
        switch (rpc_ec) {
        case ::rpc::errc::client_request_timeout:
        case ::rpc::errc::connection_timeout:
        case ::rpc::errc::disconnected_endpoint:
            return cluster::errc::timeout;
        default:
            vlog(log.error, "unexpected transform produce error: {}", rpc_ec);
            break;
        }
    } else {
        vlog(log.error, "unexpected transform produce error: {}", ec);
    }
    return cluster::errc::timeout;
}
} // namespace

class client_batcher {
    static constexpr int max_inflight = 2;
    using flush_fn
      = ss::noncopyable_function<ss::future<produce_reply>(produce_request)>;

    struct buffered_request {
        model::topic_partition tp;
        ss::chunked_fifo<model::record_batch> batches;
        ss::promise<cluster::errc> promise;
    };

    struct grouped_data {
        ss::chunked_fifo<model::record_batch> batches;
        std::vector<ss::promise<cluster::errc>> promises;

        void fail_promises(const std::exception_ptr& ep) {
            for (auto& p : promises) {
                p.set_exception(ep);
            }
        }
        void complete_promises(cluster::errc ec) {
            for (auto& p : promises) {
                p.set_value(ec);
            }
        }
    };

public:
    explicit client_batcher(flush_fn flush_impl)
      : _flush_impl(std::move(flush_impl)) {}

    ss::future<cluster::errc> enqueue(
      model::topic_partition tp,
      ss::chunked_fifo<model::record_batch> batches) {
        _buffered.emplace_back(std::move(tp), std::move(batches));
        auto fut = _buffered.back().promise.get_future();
        if (can_flush()) {
            do_flush();
        }
        return fut;
    }

    ss::future<> shutdown() {
        auto buf = std::exchange(_buffered, {});
        std::vector<ss::future<>> futures;
        std::transform(
          std::make_move_iterator(_inflight.begin()),
          std::make_move_iterator(_inflight.end()),
          std::back_inserter(futures),
          [](auto entry) { return std::move(entry.second); });
        _inflight.clear();
        co_await ss::when_all_succeed(futures.begin(), futures.end());
        for (auto& b : buf) {
            b.promise.set_exception(ss::abort_requested_exception());
        }
    }

private:
    bool can_flush() const { return _inflight.size() < max_inflight; }

    absl::btree_map<model::topic_partition, grouped_data> unbuffer_group() {
        // TODO: cap this request size
        auto buf = std::exchange(_buffered, {});
        absl::btree_map<model::topic_partition, grouped_data> grouped;
        for (auto& b : buf) {
            auto& g = grouped[b.tp];
            g.promises.push_back(std::move(b.promise));
            std::copy(
              std::make_move_iterator(b.batches.begin()),
              std::make_move_iterator(b.batches.end()),
              std::back_inserter(g.batches));
        }
        return grouped;
    }

    void do_flush() {
        if (_buffered.empty()) {
            return;
        }
        uint64_t my_id = ++latest_id;
        produce_request req;
        req.timeout = timeout;
        auto group = unbuffer_group();
        for (auto& [tp, data] : group) {
            if (
              req.topic_data.empty()
              || req.topic_data.back().topic != tp.topic) {
                req.topic_data.emplace_back(tp, std::move(data.batches));
            } else {
                req.topic_data.back().partitions.emplace_back(
                  tp.partition, std::move(data.batches));
                req.topic_data.emplace_back(tp, std::move(data.batches));
            }
        }
        _inflight.emplace(
          my_id,
          _flush_impl(std::move(req))
            .then_wrapped([this, my_id, g = std::move(group)](
                            ss::future<produce_reply> fut) mutable {
                _inflight.erase(my_id);
                if (fut.failed()) {
                    for (auto& [_, b] : g) {
                        b.fail_promises(fut.get_exception());
                    }
                    do_flush();
                    return;
                }
                auto results = fut.get().results;
                if (results.size() != g.size()) {
                    auto ep = std::make_exception_ptr(
                      std::logic_error(ss::format(
                        "response mismatch got: {} wanted: {}",
                        results.size(),
                        g.size())));
                    for (auto& [_, b] : g) {
                        b.fail_promises(ep);
                    }
                    do_flush();
                    return;
                }
                for (auto& result : results) {
                    g[result.tp].complete_promises(result.err);
                }
                do_flush();
            }));
    }

    flush_fn _flush_impl;
    uint64_t latest_id = 0;
    absl::flat_hash_map<uint64_t, ss::future<>> _inflight;
    ss::chunked_fifo<buffered_request> _buffered;
};

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
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> batches) {
    vlog(log.trace, "producing {} batches to {}", batches.size(), tp);
    auto leader = _leaders->local().get_leader(
      model::topic_namespace_view(model::kafka_namespace, tp.topic),
      tp.partition);
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    produce_request req;
    req.topic_data.emplace_back(std::move(tp), std::move(batches));
    req.timeout = timeout;
    auto reply = co_await (
      *leader == _self ? do_local_produce(std::move(req))
                       : do_remote_produce(*leader, std::move(req)));
    vassert(
      reply.results.size() == 1,
      "expected a single result: {}",
      reply.results.size());

    co_return reply.results.front().err;
}

ss::future<> client::stop() { return ss::now(); }

ss::future<produce_reply> client::do_local_produce(produce_request req) {
    auto r = co_await _local_service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(r));
}

ss::future<produce_reply>
client::do_remote_produce(model::node_id node, produce_request req) {
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [&req](impl::transform_rpc_client_protocol proto) mutable {
                        return proto.produce(
                          req.share(),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<produce_reply>);
    if (resp.has_error()) {
        cluster::errc ec = map_errc(resp.assume_error());
        produce_reply reply;
        for (const auto& topic : req.topic_data) {
            for (const auto& partition : topic.partitions) {
                reply.results.emplace_back(
                  model::topic_partition(topic.topic, partition.partition_id),
                  ec);
            }
        }
        co_return reply;
    }
    co_return std::move(resp).value();
}
} // namespace transform::rpc
