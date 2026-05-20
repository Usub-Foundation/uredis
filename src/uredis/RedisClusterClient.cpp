#include "uredis/RedisClusterClient.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <string>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis {
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

                nodes_.push_back(std::make_shared<Node>(ncfg, cfg_.max_connections_per_node));
            }
        }

        slot_to_node_.fill(0);
        standalone_mode_ = true;
    }

    void RedisClusterClient::invalidate_slot_mapping_locked() noexcept {
        if (standalone_mode_) return;
        slot_to_node_.fill(-1);
    }

    task::Awaitable<void> RedisClusterClient::force_rebuild_on_timeout() {
        auto g = co_await mutex_.lock();
        invalidate_slot_mapping_locked();
        co_return;
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
        const std::shared_ptr<Node> &node) {
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
                node->live_count.fetch_sub(1, std::memory_order_relaxed);
                node->notify_waiters_if_any();
                co_return;
            }

            if (!node->idle.try_enqueue(cli)) {
                node->live_count.fetch_sub(1, std::memory_order_relaxed);
                node->notify_waiters_if_any();
                co_return;
            }

            node->idle_sem.release();
        }
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_from_node(const std::shared_ptr<Node> &node) {
        std::shared_ptr<RedisClient> client;

        for (;;) {
            if (node->idle.try_dequeue(client)) {
                if (!client) continue;

                if (!client->connected() || !client->is_idle()) {
                    node->live_count.fetch_sub(1, std::memory_order_relaxed);
                    node->notify_waiters_if_any();
                    continue;
                }

                co_return PooledClient{node, std::move(client)};
            }

            auto cur = node->live_count.load(std::memory_order_relaxed);
            if (cur < cfg_.max_connections_per_node) {
                if (node->live_count.compare_exchange_strong(
                    cur, cur + 1,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    auto cli = std::make_shared<RedisClient>(node->cfg);
                    auto c = co_await cli->connect();
                    if (!c) {
                        node->live_count.fetch_sub(1, std::memory_order_relaxed);
                        node->notify_waiters_if_any();
                        co_return std::unexpected(c.error());
                    }
                    co_return PooledClient{node, std::move(cli)};
                }
                continue;
            }

            node->waiters.fetch_add(1, std::memory_order_relaxed);
            co_await node->idle_sem.acquire();
            node->waiters.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    task::Awaitable<void>
    RedisClusterClient::release_pooled(PooledClient &&pc, bool faulty) {
        auto node = std::move(pc.node);
        auto client = std::move(pc.client);

        if (!node || !client)
            co_return;

        if (faulty || !client->connected() || !client->is_idle()) {
            node->live_count.fetch_sub(1, std::memory_order_relaxed);
            node->notify_waiters_if_any();
            co_return;
        }

        if (!node->idle.try_enqueue(client)) {
            node->live_count.fetch_sub(1, std::memory_order_relaxed);
            node->notify_waiters_if_any();
            co_return;
        }

        node->idle_sem.release();
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_slot(int slot) {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        if (slot < 0 || slot >= 16384)
            co_return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: invalid slot"});

        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            auto idx = node_index_for_slot_locked(slot);
            if (!idx) co_return std::unexpected(idx.error());
            node = nodes_[static_cast<std::size_t>(*idx)];
        }

        co_return co_await acquire_from_node(node);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_any() {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            if (nodes_.empty())
                co_return std::unexpected(
                    RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"});
            node = nodes_.front();
        }

        co_return co_await acquire_from_node(node);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_for_key(std::string_view key) {
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        if (key.empty())
            co_return co_await acquire_for_any();

        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            auto idx = node_index_for_key_locked(key);
            if (!idx) co_return std::unexpected(idx.error());
            node = nodes_[static_cast<std::size_t>(*idx)];
        }

        co_return co_await acquire_from_node(node);
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::connect() {
        bool we_init = false;

        {
            auto g = co_await init_mutex_.lock();
            if (init_finished_)
                co_return *init_result_;
            if (!init_started_) {
                init_started_ = true;
                we_init = true;
            }
        }

        if (!we_init) {
            co_await init_event_.wait();
            co_return *init_result_;
        }

        auto res = co_await initial_discovery();

        {
            auto g = co_await init_mutex_.lock();
            init_result_ = res;
            init_finished_ = true;
            init_event_.set();
        }

        co_return res;
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::initial_discovery() {
        if (cfg_.seeds.empty())
            co_return std::unexpected(
                RedisError{RedisErrorCategory::Protocol, "RedisClusterClient: seeds list is empty"});

        if (cfg_.force_standalone) {
            std::vector<std::shared_ptr<Node> > snap;
            {
                auto g = co_await mutex_.lock();
                setup_standalone_locked();
                snap = nodes_;
            }
            for (const auto &n: snap)
                co_await warm_pool_fill_to_max(cfg_, n);
            co_return RedisResult<void>{};
        }

        {
            auto g = co_await mutex_.lock();
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

    task::Awaitable<RedisResult<void> > RedisClusterClient::rediscover_slots_serialized() {
        auto guard = co_await rediscover_mutex_.lock();

        {
            auto g = co_await mutex_.lock();
            if (standalone_mode_ || cfg_.force_standalone)
                co_return RedisResult<void>{};
        }

        co_return co_await initial_discovery();
    }

    task::Awaitable<void> RedisClusterClient::apply_moved(const Redirection &r) {
        if (r.slot < 0 || r.slot >= 16384)
            co_return;

        auto g = co_await mutex_.lock();
        int idx = ensure_node_locked(r.host, r.port);
        slot_to_node_[static_cast<std::size_t>(r.slot)] = idx;
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::execute_ask(
        const Redirection &r,
        std::string_view cmd,
        std::span<const std::string_view> args,
        int per_call_timeout_ms) {
        std::shared_ptr<Node> node;
        {
            auto g = co_await mutex_.lock();
            int idx = ensure_node_locked(r.host, r.port);
            node = nodes_[static_cast<std::size_t>(idx)];
        }

        auto pc_res = co_await acquire_from_node(node);
        if (!pc_res)
            co_return std::unexpected(pc_res.error());

        auto pc = std::move(*pc_res);

        std::span<const std::string_view> no_args;
        auto ask_resp = (per_call_timeout_ms > 0)
                            ? co_await pc.client->command_timed("ASKING", no_args, per_call_timeout_ms)
                            : co_await pc.client->command("ASKING", no_args);
        if (!ask_resp) {
            co_await release_pooled(std::move(pc), true);
            co_return std::unexpected(ask_resp.error());
        }

        auto resp = (per_call_timeout_ms > 0)
                        ? co_await pc.client->command_timed(cmd, args, per_call_timeout_ms)
                        : co_await pc.client->command(cmd, args);
        bool faulty = !resp && resp.error().category != RedisErrorCategory::ServerReply;
        co_await release_pooled(std::move(pc), faulty);

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
            auto idx = node_index_for_slot_locked(slot);
            if (!idx) co_return std::unexpected(idx.error());
            host = nodes_[static_cast<std::size_t>(*idx)]->cfg.host;
            port = nodes_[static_cast<std::size_t>(*idx)]->cfg.port;
        }

        co_return co_await connect_to_node(host, port);
    }

    static inline std::int64_t cluster_now_ms() noexcept {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
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
        auto init = co_await connect();
        if (!init) co_return std::unexpected(init.error());

        std::string key_copy;
        if (!args.empty())
            key_copy.assign(args[0].begin(), args[0].end());

        bool did_soft_rediscover = false;

        // Single monotonic deadline shared across acquire + send/recv + ASK
        // hops. 0 disables (legacy behavior, only per-IO timeout applies).
        const std::int64_t deadline_ms = (timeout_ms > 0)
                                             ? (cluster_now_ms() + static_cast<std::int64_t>(timeout_ms))
                                             : 0;

        // Lambda: budget left for a single attempt, returns -1 if no deadline,
        // 0 if expired (caller must time out).
        auto remaining = [deadline_ms]() -> int {
            if (deadline_ms <= 0) return -1;
            const auto rem = deadline_ms - cluster_now_ms();
            return rem <= 0 ? 0 : static_cast<int>(rem);
        };

        for (int attempt = 0; attempt < cfg_.max_redirections; ++attempt) {
            // Hard check the user-visible deadline before doing more work.
            if (deadline_ms > 0 && remaining() == 0) {
                co_await force_rebuild_on_timeout();
                co_return std::unexpected(
                    RedisError{RedisErrorCategory::Timeout, "RedisClusterClient: command timeout"});
            }

            PooledClient pc;
            for (;;) {
                auto ac = args.empty()
                              ? co_await acquire_for_any()
                              : co_await acquire_for_key(key_copy);

                if (ac) {
                    pc = std::move(*ac);
                    break;
                }

                if (!did_soft_rediscover && is_slot_mapping_empty_error(ac.error())) {
                    did_soft_rediscover = true;
                    auto rr = co_await rediscover_slots_serialized();
                    if (!rr)
                        co_return std::unexpected(rr.error());
                    continue;
                }

                co_return std::unexpected(ac.error());
            }

            // Compute per-call timeout for the underlying RedisClient. If we
            // have no overall deadline, pass 0 (let the client use its own
            // configured command_timeout_ms, or none if also 0).
            int per_call_to = 0;
            if (deadline_ms > 0) {
                per_call_to = remaining();
                if (per_call_to <= 0) {
                    co_await release_pooled(std::move(pc), true);
                    co_await force_rebuild_on_timeout();
                    co_return std::unexpected(
                        RedisError{RedisErrorCategory::Timeout, "RedisClusterClient: command timeout"});
                }
            }

            auto resp = (per_call_to > 0)
                            ? co_await pc.client->command_timed(cmd, args, per_call_to)
                            : co_await pc.client->command(cmd, args);
            if (resp) {
                co_await release_pooled(std::move(pc), false);
                co_return resp;
            }

            auto err = resp.error();

            // Timeout: connection is hard-closed by RedisClient already; mark
            // as faulty so it doesn't return to the idle pool, then force a
            // topology rebuild because the cluster may have moved/failed over.
            if (err.category == RedisErrorCategory::Timeout) {
                co_await release_pooled(std::move(pc), true);
                co_await force_rebuild_on_timeout();
                co_return std::unexpected(err);
            }

            if (err.category != RedisErrorCategory::ServerReply) {
                co_await release_pooled(std::move(pc), true);
                co_return std::unexpected(err);
            }

            co_await release_pooled(std::move(pc), false);

            auto redir_opt = parse_redirection(err.message);
            if (!redir_opt)
                co_return std::unexpected(err);

            const auto &redir = *redir_opt;

            if (redir.type == RedirType::Moved) {
                co_await apply_moved(redir);
                continue;
            }

            if (redir.type == RedirType::Ask) {
                int ask_to = 0;
                if (deadline_ms > 0) {
                    ask_to = remaining();
                    if (ask_to <= 0) {
                        co_await force_rebuild_on_timeout();
                        co_return std::unexpected(
                            RedisError{RedisErrorCategory::Timeout, "RedisClusterClient: command timeout"});
                    }
                }

                auto ask_resp = co_await execute_ask(redir, cmd, args, ask_to);
                if (ask_resp)
                    co_return ask_resp;

                auto err2 = ask_resp.error();
                if (err2.category == RedisErrorCategory::Timeout) {
                    co_await force_rebuild_on_timeout();
                    co_return std::unexpected(err2);
                }

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
