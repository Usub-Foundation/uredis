#include "uredis/RedisClusterClient.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <chrono>
#include <exception>
#include <string>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis {
    static inline std::int64_t cluster_now_ms() noexcept {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    // The calling task was cancelled (a lock/semaphore/event wait came back
    // empty). Says nothing about the cluster: never cached, never shared.
    static RedisError cancelled_error() {
        return RedisError{RedisErrorCategory::Io, "RedisClusterClient: cancelled"};
    }

    static RedisError command_timeout_error(const char *where) {
        return RedisError{RedisErrorCategory::Timeout, std::string("RedisClusterClient: command timeout") + where};
    }

    static bool is_cluster_disabled_error(const RedisError &e) {
        if (e.category != RedisErrorCategory::ServerReply)
            return false;

        std::string m = e.message;
        for (auto &c: m)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));

        return m.find("cluster support disabled") != std::string::npos
               || m.find("not in cluster mode") != std::string::npos
               || m.find("cluster is disabled") != std::string::npos
               || m.find("unknown command") != std::string::npos
               || m.find("unknown subcommand") != std::string::npos
               || (m.find("cluster") != std::string::npos &&
                   m.find("unknown") != std::string::npos);
    }

    bool RedisClusterClient::is_slot_mapping_empty_error(const RedisError &e) noexcept {
        if (e.category != RedisErrorCategory::Protocol)
            return false;

        return e.message.find("slot mapping is empty") != std::string::npos
               || e.message.find("incomplete slot coverage") != std::string::npos
               || e.message.find("CLUSTER SLOTS returned no slot ranges") != std::string::npos;
    }

    RedisClusterClient::RedisClusterClient(RedisClusterConfig cfg)
        : cfg_(std::move(cfg)) {
        slot_to_node_.fill(-1);

        if (cfg_.max_redirections <= 0)
            cfg_.max_redirections = 5;
        if (cfg_.max_connections_per_node == 0)
            cfg_.max_connections_per_node = 1;

        normalize_auth(cfg_.username);
        normalize_auth(cfg_.password);

#ifdef UREDIS_LOGS
        ulog::fatal("RedisClusterClient ctor: pass={}",
                    cfg_.password.has_value());
#endif
    }

    std::string_view RedisClusterClient::extract_hash_tag(std::string_view key) {
        auto l = key.find('{');
        if (l == std::string_view::npos) return key;
        auto r = key.find('}', l + 1);
        if (r == std::string_view::npos || r == l + 1) return key;
        return key.substr(l + 1, r - l - 1);
    }

    std::uint16_t RedisClusterClient::calc_slot(std::string_view key) {
        if (key.empty()) return 0;

        std::uint16_t crc = 0;
        for (unsigned char b: key) {
            crc ^= static_cast<std::uint16_t>(b) << 8;
            for (int i = 0; i < 8; ++i) {
                if (crc & 0x8000)
                    crc = static_cast<std::uint16_t>((crc << 1) ^ 0x1021);
                else
                    crc = static_cast<std::uint16_t>(crc << 1);
            }
        }
        return static_cast<std::uint16_t>(crc % 16384);
    }

    std::optional<RedisClusterClient::Redirection>
    RedisClusterClient::parse_redirection(const std::string &msg) {
        std::string_view s{msg};

        auto next_token = [](std::string_view &str) -> std::string_view {
            while (!str.empty() && str.front() == ' ')
                str.remove_prefix(1);
            if (str.empty()) return {};
            auto pos = str.find(' ');
            if (pos == std::string_view::npos) {
                auto t = str;
                str = {};
                return t;
            }
            auto t = str.substr(0, pos);
            str.remove_prefix(pos + 1);
            return t;
        };

        auto t0 = next_token(s);
        if (t0.empty()) return std::nullopt;

        RedirType type;
        if (t0 == "MOVED") type = RedirType::Moved;
        else if (t0 == "ASK") type = RedirType::Ask;
        else return std::nullopt;

        auto t1 = next_token(s);
        auto t2 = next_token(s);
        if (t1.empty() || t2.empty()) return std::nullopt;

        int slot = 0;
        {
            auto [p, ec] = std::from_chars(t1.data(), t1.data() + t1.size(), slot);
            (void) p;
            if (ec != std::errc{}) return std::nullopt;
        }

        auto colon = t2.find(':');
        if (colon == std::string_view::npos) return std::nullopt;

        std::string host{t2.substr(0, colon)};
        int port_i = 0;
        {
            auto [p, ec] = std::from_chars(
                t2.data() + colon + 1, t2.data() + t2.size(), port_i);
            (void) p;
            if (ec != std::errc{}) return std::nullopt;
        }
        if (port_i <= 0 || port_i > 65535) return std::nullopt;

        return Redirection{
            type, slot, std::move(host), static_cast<std::uint16_t>(port_i)
        };
    }

    RedisResult<int> RedisClusterClient::node_index_for_slot_locked(int slot) const {
        if (slot < 0 || slot >= 16384)
            return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: invalid slot"});

        int idx = slot_to_node_[static_cast<std::size_t>(slot)];
        if (idx < 0 || static_cast<std::size_t>(idx) >= nodes_.size())
            return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: slot mapping is empty"});

        return idx;
    }

    RedisResult<int> RedisClusterClient::node_index_for_key_locked(std::string_view key) const {
        if (nodes_.empty())
            return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"});

        if (key.empty())
            return 0;

        auto tag = extract_hash_tag(key);
        auto slot = calc_slot(tag);
        return node_index_for_slot_locked(static_cast<int>(slot));
    }

    int RedisClusterClient::ensure_node_locked(std::string_view host, std::uint16_t port) {
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            if (nodes_[i]->cfg.host == host && nodes_[i]->cfg.port == port)
                return static_cast<int>(i);
        }

        RedisConfig ncfg;
        ncfg.host = std::string(host);
        ncfg.port = port;
        ncfg.db = 0;
        ncfg.username = cfg_.username;
        ncfg.password = cfg_.password;
        ncfg.connect_timeout_ms = cfg_.connect_timeout_ms;
        ncfg.io_timeout_ms = cfg_.io_timeout_ms;
        ncfg.command_timeout_ms = cfg_.command_timeout_ms;
        ncfg.tls = cfg_.tls;

        nodes_.push_back(std::make_shared<Node>(ncfg, cfg_.max_connections_per_node));
        return static_cast<int>(nodes_.size() - 1);
    }

    void RedisClusterClient::setup_standalone_locked() {
        if (nodes_.empty()) {
            for (const auto &s: cfg_.seeds) {
                RedisConfig ncfg;
                ncfg.host = s.host;
                ncfg.port = s.port;
                ncfg.db = 0;
                ncfg.username = cfg_.username;
                ncfg.password = cfg_.password;
                ncfg.connect_timeout_ms = cfg_.connect_timeout_ms;
                ncfg.io_timeout_ms = cfg_.io_timeout_ms;
                ncfg.command_timeout_ms = cfg_.command_timeout_ms;
                ncfg.tls = cfg_.tls;

                nodes_.push_back(std::make_shared<Node>(ncfg, cfg_.max_connections_per_node));
            }
        }

        slot_to_node_.fill(0);
        standalone_mode_ = true;
        topology_gen_.fetch_add(1, std::memory_order_acq_rel);
    }

    bool RedisClusterClient::has_full_slot_mapping_locked() const noexcept {
        return std::all_of(
            slot_to_node_.begin(), slot_to_node_.end(),
            [](int x) { return x >= 0; });
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::connect_to_node(std::string_view host, std::uint16_t port) {
        RedisConfig cfg;
        cfg.host = std::string(host);
        cfg.port = port;
        cfg.db = 0;
        cfg.username = cfg_.username;
        cfg.password = cfg_.password;
        cfg.connect_timeout_ms = cfg_.connect_timeout_ms;
        cfg.io_timeout_ms = cfg_.io_timeout_ms;
        cfg.command_timeout_ms = cfg_.command_timeout_ms;
        cfg.tls = cfg_.tls;

#ifdef UREDIS_LOGS
        ulog::warn("connect_to_node: cluster_pass={} local_pass={}",
                   cfg_.password.has_value(), cfg.password.has_value());
#endif

        auto cli = std::make_shared<RedisClient>(cfg);
        auto c = co_await cli->connect();
        if (!c)
            co_return std::unexpected(c.error());
        co_return cli;
    }

    task::Awaitable<void> RedisClusterClient::warm_pool_fill_to_max(
        const RedisClusterConfig &cfg,
        std::shared_ptr<Node> node) {
        for (;;) {
            auto cur = node->live_count.load(std::memory_order_relaxed);
            if (cur >= cfg.max_connections_per_node)
                co_return;

            if (!node->live_count.compare_exchange_strong(
                cur, cur + 1,
                std::memory_order_acq_rel, std::memory_order_relaxed))
                continue;

            auto cli = std::make_shared<RedisClient>(node->cfg);
            auto c = co_await cli->connect();
            if (!c) {
                node->drop_live();
                co_return;
            }

            if (!node->push_idle(std::move(cli))) {
                node->drop_live();
                co_return;
            }
        }
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_from_node(std::shared_ptr<Node> node, std::int64_t deadline_ms) {
        std::shared_ptr<RedisClient> client;

        for (;;) {
            if (node->idle.try_dequeue(client)) {
                if (!client) continue;

                if (!client->connected() || !client->is_idle()) {
                    client.reset();
                    node->drop_live();
                    continue;
                }

                co_return PooledClient{node, std::move(client)};
            }

            auto cur = node->live_count.load(std::memory_order_seq_cst);
            if (cur < cfg_.max_connections_per_node) {
                if (node->live_count.compare_exchange_strong(
                    cur, cur + 1,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    auto cli = std::make_shared<RedisClient>(node->cfg);
                    auto c = co_await cli->connect();
                    if (!c) {
                        node->drop_live();
                        co_return std::unexpected(c.error());
                    }
                    co_return PooledClient{node, std::move(cli)};
                }
                continue;
            }

            // Pool exhausted. Announce ourselves, then look again: a release
            // that ran before the announcement saw no waiter and woke nobody.
            node->waiters.fetch_add(1, std::memory_order_seq_cst);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (node->idle.try_dequeue(client)) {
                node->waiters.fetch_sub(1, std::memory_order_relaxed);
                if (!client) continue;
                if (!client->connected() || !client->is_idle()) {
                    client.reset();
                    node->drop_live();
                    continue;
                }
                co_return PooledClient{node, std::move(client)};
            }
            if (node->live_count.load(std::memory_order_seq_cst) < cfg_.max_connections_per_node) {
                node->waiters.fetch_sub(1, std::memory_order_relaxed);
                continue;
            }

            bool woke = false;
            bool timed_out = false;
            if (deadline_ms > 0) {
                const std::int64_t rem = deadline_ms - cluster_now_ms();
                if (rem <= 0) {
                    timed_out = true;
                } else {
                    auto r = co_await sync::select(
                        node->idle_sem.acquire_op(),
                        sync::sleep_op(std::chrono::milliseconds{rem}));
                    if (!r.cancelled()) {
                        if (r.index == 0)
                            woke = true;
                        else
                            timed_out = true;
                    }
                }
            } else {
                woke = co_await node->idle_sem.acquire();
            }
            node->waiters.fetch_sub(1, std::memory_order_relaxed);

            // Nothing was taken from the pool: no connection to give back.
            if (timed_out)
                co_return std::unexpected(command_timeout_error(" (waiting for a pooled connection)"));
            if (!woke)
                co_return std::unexpected(cancelled_error());
        }
    }

    void RedisClusterClient::release_pooled(PooledClient &&pc, bool faulty) noexcept {
        auto node = std::move(pc.node);
        auto client = std::move(pc.client);

        if (!node || !client)
            return;

        // Faulty = IO failure or a timed-out in-flight command (RedisClient has
        // already hard-closed it); a dead or half-closed socket goes too.
        // Everything else — including a connection the caller acquired but
        // never used because its deadline ran out — is reused.
        if (faulty || !client->connected() || !client->is_idle()) {
            client.reset();
            node->drop_live();
            return;
        }

        if (!node->push_idle(std::move(client)))
            node->drop_live();
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_slot(int slot, std::int64_t deadline_ms) {
        if (slot < 0 || slot >= 16384)
            co_return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: invalid slot"});

        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            auto idx = node_index_for_slot_locked(slot);
            if (!idx) co_return std::unexpected(idx.error());
            node = nodes_[static_cast<std::size_t>(*idx)];
        }

        co_return co_await acquire_from_node(std::move(node), deadline_ms);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_any(std::int64_t deadline_ms) {
        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            if (nodes_.empty())
                co_return std::unexpected(
                    RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"});
            node = nodes_.front();
        }

        co_return co_await acquire_from_node(std::move(node), deadline_ms);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_key(std::string_view key, std::int64_t deadline_ms) {
        if (key.empty())
            co_return co_await acquire_for_any(deadline_ms);

        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            auto idx = node_index_for_key_locked(key);
            if (!idx) co_return std::unexpected(idx.error());
            node = nodes_[static_cast<std::size_t>(*idx)];
        }

        co_return co_await acquire_from_node(std::move(node), deadline_ms);
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::connect() {
        if (init_ok_.load(std::memory_order_acquire))
            co_return RedisResult<void>{};
        co_return co_await run_discovery(true, 0, 0);
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::run_discovery(
        bool initial, std::uint64_t observed_gen, std::int64_t deadline_ms) {
        for (;;) {
            // A cancelled caller must not lead a run: its failure would be
            // handed to every joiner.
            if (usub::uvent::system::this_coroutine::cancel_requested())
                co_return std::unexpected(cancelled_error());

            std::shared_ptr<DiscoveryFlight> flight;
            bool leader = false;
            bool satisfied = false;
            {
                std::lock_guard<std::mutex> lk(discovery_mu_);
                if (initial)
                    satisfied = init_ok_.load(std::memory_order_acquire);
                else
                    satisfied = topology_gen_.load(std::memory_order_acquire) != observed_gen;

                if (!satisfied) {
                    if (discovery_flight_) {
                        flight = discovery_flight_;
                    } else {
                        flight = std::make_shared<DiscoveryFlight>();
                        discovery_flight_ = flight;
                        leader = true;
                    }
                }
            }
            if (satisfied)
                co_return RedisResult<void>{};

            if (leader) {
                RedisResult<void> res = std::unexpected(
                    RedisError{RedisErrorCategory::Io, "RedisClusterClient: discovery failed"});
                try {
                    res = co_await initial_discovery();
                } catch (const std::exception &ex) {
                    res = std::unexpected(RedisError{
                        RedisErrorCategory::Io, std::string("RedisClusterClient: discovery threw: ") + ex.what()
                    });
                } catch (...) {
                    res = std::unexpected(RedisError{
                        RedisErrorCategory::Io, "RedisClusterClient: discovery threw"
                    });
                }
                const bool aborted = !res && usub::uvent::system::this_coroutine::cancel_requested();

                {
                    std::lock_guard<std::mutex> lk(discovery_mu_);
                    // Only success is remembered; after a failure the next
                    // caller starts a new run.
                    if (res)
                        init_ok_.store(true, std::memory_order_release);
                    flight->result = res;
                    flight->aborted = aborted;
                    discovery_flight_.reset();
                }
                flight->done.set();
                co_return res;
            }

            bool woke = false;
            bool timed_out = false;
            if (flight->done.is_set()) {
                woke = true;
            } else if (deadline_ms > 0) {
                const std::int64_t rem = deadline_ms - cluster_now_ms();
                if (rem <= 0) {
                    timed_out = true;
                } else {
                    auto r = co_await sync::select(
                        flight->done.wait_op(),
                        sync::sleep_op(std::chrono::milliseconds{rem}));
                    if (!r.cancelled()) {
                        if (r.index == 0)
                            woke = true;
                        else
                            timed_out = true;
                    }
                }
            } else {
                woke = co_await flight->done.wait();
            }

            if (timed_out)
                co_return std::unexpected(command_timeout_error(" (waiting for cluster discovery)"));
            if (!woke)
                co_return std::unexpected(cancelled_error());
            if (flight->aborted)
                continue;
            co_return flight->result;
        }
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::initial_discovery() {
        if (cfg_.seeds.empty())
            co_return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: seeds list is empty"});

        if (cfg_.force_standalone) {
            std::vector<std::shared_ptr<Node> > snap;
            {
                auto g = co_await mutex_.lock();
                if (!g.owns_lock())
                    co_return std::unexpected(cancelled_error());
                setup_standalone_locked();
                snap = nodes_;
            }
            for (const auto &n: snap)
                co_await warm_pool_fill_to_max(cfg_, n);
            co_return RedisResult<void>{};
        }

        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            if (standalone_mode_) {
                auto snap = nodes_;
                g.unlock();
                for (const auto &n: snap)
                    co_await warm_pool_fill_to_max(cfg_, n);
                co_return RedisResult<void>{};
            }
        }

        RedisError last_err{RedisErrorCategory::Io, "no attempts"};

        for (const auto &seed: cfg_.seeds) {
            auto mc = co_await connect_to_node(seed.host, seed.port);
            if (!mc) {
                last_err = mc.error();
                continue;
            }

            auto &client = *mc;

            std::string_view slot_arg = "SLOTS";
            std::array<std::string_view, 1> args_arr{slot_arg};
            auto resp = co_await client->command(
                "CLUSTER",
                std::span<const std::string_view>(args_arr.data(), args_arr.size()));

            if (!resp) {
                const auto &e = resp.error();
                last_err = e;

                if (is_cluster_disabled_error(e)) {
                    std::vector<std::shared_ptr<Node> > snap;
                    {
                        auto g = co_await mutex_.lock();
                        if (!g.owns_lock())
                            co_return std::unexpected(cancelled_error());
                        setup_standalone_locked();
                        snap = nodes_;
                    }
                    for (const auto &n: snap)
                        co_await warm_pool_fill_to_max(cfg_, n);
                    co_return RedisResult<void>{};
                }
                continue;
            }

            const RedisValue &v = *resp;
            if (!v.is_array()) {
                last_err = RedisError{
                    RedisErrorCategory::Protocol,
                    "RedisClusterClient: CLUSTER SLOTS reply not array"
                };
                continue;
            }

            const auto &slot_ranges = v.as_array();

            if (slot_ranges.empty()) {
                std::vector<std::shared_ptr<Node> > snap;
                {
                    auto g = co_await mutex_.lock();
                    if (!g.owns_lock())
                        co_return std::unexpected(cancelled_error());
                    setup_standalone_locked();
                    snap = nodes_;
                }
                for (const auto &n: snap)
                    co_await warm_pool_fill_to_max(cfg_, n);
                co_return RedisResult<void>{};
            }

            auto parse_i64 = [](const RedisValue &x) -> std::optional<int64_t> {
                if (auto vv = x.as_optional_integer()) return *vv;
                if (x.is_bulk_string() || x.is_simple_string()) {
                    auto s = x.as_string();
                    int64_t out = 0;
                    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
                    (void) p;
                    if (ec == std::errc{}) return out;
                }
                return std::nullopt;
            };

            auto parse_host = [&seed](const RedisValue &x) -> std::optional<std::string> {
                if (x.is_null()) return std::string(seed.host);
                if (x.is_bulk_string() || x.is_simple_string()) {
                    auto h = x.as_string();
                    if (h.empty()) h = seed.host;
                    return h;
                }
                return std::nullopt;
            };

            bool ok_mapping = false;
            std::vector<std::shared_ptr<Node> > nodes_snapshot;
            std::array<int, 16384> new_map{};
            new_map.fill(-1);

            {
                auto guard = co_await mutex_.lock();
                if (!guard.owns_lock())
                    co_return std::unexpected(cancelled_error());

                auto ensure = [&](const RedisValue &node_val) -> std::optional<int> {
                    if (!node_val.is_array()) return std::nullopt;
                    const auto &arr = node_val.as_array();
                    if (arr.size() < 2) return std::nullopt;

                    auto host_opt = parse_host(arr[0]);
                    auto port_opt = parse_i64(arr[1]);
                    if (!host_opt || !port_opt) return std::nullopt;

                    auto host = std::move(*host_opt);
                    auto port_i = *port_opt;
                    if (port_i <= 0 || port_i > 65535) return std::nullopt;

                    auto port = static_cast<std::uint16_t>(port_i);

#ifdef UREDIS_PORT_FORWARD_SUPPORT
                    if (host == seed.host && port != seed.port)
                        port = seed.port;
#endif

                    return ensure_node_locked(host, port);
                };

                for (const auto &range_val: slot_ranges) {
                    if (!range_val.is_array()) continue;
                    const auto &range_arr = range_val.as_array();
                    if (range_arr.size() < 3) continue;

                    auto start_opt = parse_i64(range_arr[0]);
                    auto end_opt = parse_i64(range_arr[1]);
                    if (!start_opt || !end_opt) continue;

                    auto start = *start_opt;
                    auto end = *end_opt;

                    auto master_idx = ensure(range_arr[2]);
                    if (!master_idx) continue;

                    if (start < 0) start = 0;
                    if (end > 16383) end = 16383;
                    if (end < start) continue;

                    for (int64_t s = start; s <= end; ++s)
                        new_map[static_cast<std::size_t>(s)] = *master_idx;

                    for (std::size_t i = 3; i < range_arr.size(); ++i)
                        (void) ensure(range_arr[i]);
                }

                bool full = std::all_of(
                    new_map.begin(), new_map.end(),
                    [](int m) { return m >= 0; });

                if (full) {
                    slot_to_node_ = new_map;
                    standalone_mode_ = false;
                    ok_mapping = true;
                    nodes_snapshot = nodes_;
                    topology_gen_.fetch_add(1, std::memory_order_acq_rel);
                } else {
                    if (!has_full_slot_mapping_locked()) {
                        setup_standalone_locked();
                        ok_mapping = true;
                        nodes_snapshot = nodes_;
                    }
                }
            }

            if (!ok_mapping) {
                last_err = RedisError{
                    RedisErrorCategory::Protocol,
                    "RedisClusterClient: CLUSTER SLOTS returned incomplete slot coverage"
                };
                continue;
            }

            for (const auto &n: nodes_snapshot)
                co_await warm_pool_fill_to_max(cfg_, n);

            co_return RedisResult<void>{};
        }

        co_return std::unexpected(RedisError{
            last_err.category,
            std::string("RedisClusterClient: CLUSTER SLOTS failed on all seeds; last=") + last_err.message
        });
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::rediscover_slots_serialized(
        std::uint64_t observed_gen, std::int64_t deadline_ms) {
        if (cfg_.force_standalone)
            co_return RedisResult<void>{};
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            if (standalone_mode_)
                co_return RedisResult<void>{};
        }

        // Concurrent callers that saw the same generation share one CLUSTER
        // SLOTS run; callers arriving after it succeeded see a newer
        // generation and just retry on the rebuilt map.
        co_return co_await run_discovery(false, observed_gen, deadline_ms);
    }

    task::Awaitable<void> RedisClusterClient::apply_moved(Redirection r) {
        if (r.slot < 0 || r.slot >= 16384)
            co_return;

        auto g = co_await mutex_.lock();
        if (!g.owns_lock())
            co_return;
        int idx = ensure_node_locked(r.host, r.port);
        slot_to_node_[static_cast<std::size_t>(r.slot)] = idx;
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::execute_ask(
        Redirection r,
        std::string_view cmd,
        std::span<const std::string_view> args,
        std::int64_t deadline_ms) {
        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            int idx = ensure_node_locked(r.host, r.port);
            node = nodes_[static_cast<std::size_t>(idx)];
        }

        auto pc_res = co_await acquire_from_node(std::move(node), deadline_ms);
        if (!pc_res)
            co_return std::unexpected(pc_res.error());

        auto pc = std::move(*pc_res);

        // Budget left for one call on the acquired connection: -1 = no
        // deadline, 0 = expired before anything was sent.
        auto budget = [deadline_ms]() -> int {
            if (deadline_ms <= 0) return -1;
            const auto rem = deadline_ms - cluster_now_ms();
            return rem <= 0 ? 0 : static_cast<int>(rem);
        };

        int to = budget();
        if (to == 0) {
            release_pooled(std::move(pc), false); // never used
            co_return std::unexpected(command_timeout_error(""));
        }

        std::span<const std::string_view> no_args;
        RedisResult<RedisValue> ask_resp;
        if (to > 0)
            ask_resp = co_await pc.client->command_timed("ASKING", no_args, to);
        else
            ask_resp = co_await pc.client->command("ASKING", no_args);
        if (!ask_resp) {
            const bool faulty = ask_resp.error().category != RedisErrorCategory::ServerReply;
            release_pooled(std::move(pc), faulty);
            co_return std::unexpected(ask_resp.error());
        }

        to = budget();
        if (to == 0) {
            // ASKING was answered, nothing is in flight: the connection is
            // fine (the ASKING flag only applies to the next command).
            release_pooled(std::move(pc), false);
            co_return std::unexpected(command_timeout_error(""));
        }

        RedisResult<RedisValue> resp;
        if (to > 0)
            resp = co_await pc.client->command_timed(cmd, args, to);
        else
            resp = co_await pc.client->command(cmd, args);
        const bool faulty = !resp && resp.error().category != RedisErrorCategory::ServerReply;
        release_pooled(std::move(pc), faulty);

        co_return resp;
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_client_for_key(std::string_view key) {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        if (key.empty())
            co_return co_await get_any_client();

        std::string host;
        std::uint16_t port;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            auto idx = node_index_for_key_locked(key);
            if (!idx) co_return std::unexpected(idx.error());
            host = nodes_[static_cast<std::size_t>(*idx)]->cfg.host;
            port = nodes_[static_cast<std::size_t>(*idx)]->cfg.port;
        }

        co_return co_await connect_to_node(host, port);
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_any_client() {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        std::string host;
        std::uint16_t port;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            if (nodes_.empty())
                co_return std::unexpected(
                    RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"});
            host = nodes_.front()->cfg.host;
            port = nodes_.front()->cfg.port;
        }

        co_return co_await connect_to_node(host, port);
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_client_for_slot(int slot) {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        std::string host;
        std::uint16_t port;
        {
            auto g = co_await mutex_.lock();
            if (!g.owns_lock())
                co_return std::unexpected(cancelled_error());
            auto idx = node_index_for_slot_locked(slot);
            if (!idx) co_return std::unexpected(idx.error());
            host = nodes_[static_cast<std::size_t>(*idx)]->cfg.host;
            port = nodes_[static_cast<std::size_t>(*idx)]->cfg.port;
        }

        co_return co_await connect_to_node(host, port);
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::command(
        std::string_view cmd,
        std::span<const std::string_view> args) {
        co_return co_await command_impl(cmd, args, cfg_.command_timeout_ms);
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::command_timed(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms) {
        co_return co_await command_impl(cmd, args, timeout_ms);
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::command_impl(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms) {
        // Single monotonic deadline shared across discovery wait, acquire,
        // send/recv and ASK/MOVED hops. 0 disables it (only per-IO timeout
        // applies).
        const std::int64_t deadline_ms = (timeout_ms > 0)
                                             ? (cluster_now_ms() + static_cast<std::int64_t>(timeout_ms))
                                             : 0;

        // Not connected yet, or every discovery so far failed (seeds were
        // down at startup): run one now — or join the one in flight.
        if (!init_ok_.load(std::memory_order_acquire)) {
            auto init = co_await run_discovery(true, 0, deadline_ms);
            if (!init) co_return std::unexpected(init.error());
        }

        std::string key_copy;
        if (!args.empty())
            key_copy.assign(args[0].begin(), args[0].end());

        bool did_soft_rediscover = false;
        // One free topology rebuild per command for Io failures: a node that
        // died or changed address (k8s pod restart, failover promoting the
        // replica) keeps its stale entry in the cached slot map, and without a
        // rediscover every command routed to it fails forever. MOVED redirects
        // don't cover this case — a dead node sends nothing at all.
        bool did_conn_rediscover = false;

        // Budget left for one attempt: -1 = no deadline, 0 = expired.
        auto remaining = [deadline_ms]() -> int {
            if (deadline_ms <= 0) return -1;
            const auto rem = deadline_ms - cluster_now_ms();
            return rem <= 0 ? 0 : static_cast<int>(rem);
        };

        for (int attempt = 0; attempt < cfg_.max_redirections; ++attempt) {
            if (deadline_ms > 0 && remaining() == 0)
                co_return std::unexpected(command_timeout_error(""));

            PooledClient pc;
            // Topology generation this attempt is routed with. A rebuild
            // request carries it, so callers that failed on the same map share
            // one CLUSTER SLOTS run and callers that failed on an already
            // replaced map just retry.
            std::uint64_t routed_gen = 0;
            for (;;) {
                routed_gen = topology_gen_.load(std::memory_order_acquire);
                RedisResult<PooledClient> ac;
                if (args.empty())
                    ac = co_await acquire_for_any(deadline_ms);
                else
                    ac = co_await acquire_for_key(key_copy, deadline_ms);

                if (ac) {
                    pc = std::move(*ac);
                    break;
                }

                RedisError aerr = ac.error();

                // Deadline passed while waiting for a free connection: nothing
                // was taken, nothing to close, the topology is not suspect.
                if (aerr.category == RedisErrorCategory::Timeout)
                    co_return std::unexpected(std::move(aerr));

                if (!did_soft_rediscover && is_slot_mapping_empty_error(aerr)) {
                    did_soft_rediscover = true;
                    auto rr = co_await rediscover_slots_serialized(routed_gen, deadline_ms);
                    if (!rr)
                        co_return std::unexpected(rr.error());
                    continue;
                }

                // Node unreachable (connect refused/reset — stale address after
                // a failover or pod restart): rebuild the topology from the
                // seeds once and retry the acquire on the fresh map.
                if (!did_conn_rediscover && aerr.category == RedisErrorCategory::Io) {
                    did_conn_rediscover = true;
                    auto rr = co_await rediscover_slots_serialized(routed_gen, deadline_ms);
                    if (rr)
                        continue;
                }

                co_return std::unexpected(std::move(aerr));
            }

            // Per-call timeout for the underlying RedisClient. Without an
            // overall deadline pass 0 (the client uses its own
            // command_timeout_ms, or none).
            int per_call_to = 0;
            if (deadline_ms > 0) {
                per_call_to = remaining();
                if (per_call_to <= 0) {
                    // The budget ran out while waiting for the connection or a
                    // rediscovery. Nothing was sent on it — it is healthy and
                    // goes back to the pool.
                    release_pooled(std::move(pc), false);
                    co_return std::unexpected(command_timeout_error(""));
                }
            }

            RedisResult<RedisValue> resp;
            if (per_call_to > 0)
                resp = co_await pc.client->command_timed(cmd, args, per_call_to);
            else
                resp = co_await pc.client->command(cmd, args);
            if (resp) {
                release_pooled(std::move(pc), false);
                co_return resp;
            }

            auto err = resp.error();

            // The in-flight command timed out: RedisClient has hard-closed the
            // connection (a late reply would desync the stream), so it leaves
            // the pool. The slot map is left alone: a slow reply says nothing
            // about slot ownership, and invalidating the map on every timeout
            // turned a slow node into a rediscovery storm for all concurrent
            // commands. A node that is really gone fails the next connect with
            // an Io error, which rebuilds the topology (single-flighted).
            if (err.category == RedisErrorCategory::Timeout) {
                release_pooled(std::move(pc), true);
                co_return std::unexpected(err);
            }

            if (err.category != RedisErrorCategory::ServerReply) {
                release_pooled(std::move(pc), true);
                // Connection died mid-command: one-shot topology rebuild +
                // retry, like the acquire path. The retry may double-send a
                // command the dying node already applied — same at-least-once
                // guarantee as ASK/MOVED retries, callers must stick to
                // idempotent commands.
                if (!did_conn_rediscover &&
                    err.category == RedisErrorCategory::Io) {
                    did_conn_rediscover = true;
                    auto rr = co_await rediscover_slots_serialized(routed_gen, deadline_ms);
                    if (rr)
                        continue;
                }
                co_return std::unexpected(err);
            }

            release_pooled(std::move(pc), false);

            auto redir_opt = parse_redirection(err.message);
            if (!redir_opt)
                co_return std::unexpected(err);

            if (redir_opt->type == RedirType::Moved) {
                co_await apply_moved(*redir_opt);
                continue;
            }

            if (redir_opt->type == RedirType::Ask) {
                if (deadline_ms > 0 && remaining() == 0)
                    co_return std::unexpected(command_timeout_error(""));

                auto ask_resp = co_await execute_ask(*redir_opt, cmd, args, deadline_ms);
                if (ask_resp)
                    co_return ask_resp;

                auto err2 = ask_resp.error();
                if (err2.category == RedisErrorCategory::Timeout)
                    co_return std::unexpected(err2);

                auto redir2 = parse_redirection(err2.message);
                if (redir2 && redir2->type == RedirType::Moved) {
                    co_await apply_moved(*redir2);
                    continue;
                }

                co_return std::unexpected(err2);
            }

            co_return std::unexpected(err);
        }

        co_return std::unexpected(
            RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: too many redirections"});
    }
} // namespace usub::uredis
