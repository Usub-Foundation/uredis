#include "uredis/RedisClient.h"

#include <chrono>
#include <cstring>
#include <string>
#include <array>
#include <cctype>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis {
    using usub::uvent::utils::DynamicBuffer;

    static inline std::uintptr_t ptr_id(const void *p) noexcept {
        return reinterpret_cast<std::uintptr_t>(p);
    }

    static inline std::array<unsigned, 4> tail4(const std::string &s) {
        const auto n = s.size();
        auto b = [&](std::size_t i) -> unsigned {
            return static_cast<unsigned>(static_cast<unsigned char>(s[i]));
        };
        return {
            n > 3 ? b(n - 4) : 0u,
            n > 2 ? b(n - 3) : 0u,
            n > 1 ? b(n - 2) : 0u,
            n > 0 ? b(n - 1) : 0u
        };
    }

    RedisClient::RedisClient(RedisConfig cfg)
        : config_(std::move(cfg)) {
        normalize_auth(config_.username);
        normalize_auth(config_.password);

#ifdef UREDIS_LOGS
        ulog::debug("RedisClient::ctor: this={} host=\"{}\" port={} db={} user_set={} pass_set={}",
                    ptr_id(this), config_.host, config_.port, config_.db,
                    config_.username.has_value(), config_.password.has_value());

        if (config_.password) {
            const auto &p = *config_.password;
            auto t = tail4(p);
            ulog::info("RedisClient::password-meta: len={} tail=[{:02x} {:02x} {:02x} {:02x}]",
                       p.size(), t[0], t[1], t[2], t[3]);
        }
#endif
    }

    RedisClient::~RedisClient() {
#ifdef UREDIS_LOGS
        ulog::debug("RedisClient::dtor: this={} connected={} closing={} socket={}",
                    ptr_id(this), connected_, closing_, ptr_id(socket_.get()));
#endif
        hard_close_socket_unlocked();
    }

    bool RedisClient::is_idle() const noexcept {
        return connected_ && !closing_;
    }

    void RedisClient::hard_close_socket_unlocked() noexcept {
        closing_ = true;
        connected_ = false;

        parser_.reset();

        auto sock = socket_;
        if (sock) {
#ifdef UREDIS_LOGS
            ulog::warn("RedisClient::hard_close_socket: this={} socket={}", ptr_id(this), ptr_id(sock.get()));
#endif
            sock->shutdown();
        }
        socket_.reset();
    }

    task::Awaitable<RedisResult<void> > RedisClient::connect() {
        co_return co_await connect_unlocked();
    }

    task::Awaitable<RedisResult<void> > RedisClient::connect_unlocked() {
        if (connected_ && socket_ && !closing_)
            co_return RedisResult<void>{};

        normalize_auth(config_.username);
        normalize_auth(config_.password);

        for (int attempt = 0; attempt < 2; ++attempt) {
            closing_ = false;
            connected_ = false;
            parser_.reset();
            socket_.reset();
            socket_ = std::make_shared<net::TCPClientSocket>();

            const std::string port_str = std::to_string(config_.port);

#ifdef UREDIS_LOGS
            ulog::info(
                "RedisClient::connect: attempt={} this={} host=\"{}\" port={} db={} socket={} user_set={} pass_set={}",
                attempt, ptr_id(this), config_.host, config_.port, config_.db, ptr_id(socket_.get()),
                config_.username.has_value(), config_.password.has_value());
#endif

            auto rc = co_await socket_->async_connect(config_.host.c_str(), port_str.c_str());
            if (rc.has_value()) {
#ifdef UREDIS_LOGS
                ulog::error("RedisClient::connect: async_connect failed this={} host=\"{}\" port={} attempt={}",
                            ptr_id(this), config_.host, config_.port, attempt);
#endif
                hard_close_socket_unlocked();
                if (attempt == 0) continue;
                co_return std::unexpected(RedisError{RedisErrorCategory::Io, "async_connect failed"});
            }

            connected_ = true;

            auto auth = co_await auth_and_select_unlocked();
            if (!auth) {
#ifdef UREDIS_LOGS
                ulog::error(
                    R"(RedisClient::connect: AUTH/SELECT failed this={} host="{}" port={} category={} msg="{}")",
                    ptr_id(this), config_.host, config_.port,
                    (int) auth.error().category, auth.error().message);
#endif
                hard_close_socket_unlocked();
                if (attempt == 0) continue;
                co_return std::unexpected(auth.error());
            }

#ifdef UREDIS_LOGS
            ulog::info("RedisClient::connect: OK this={} socket={}", ptr_id(this), ptr_id(socket_.get()));
#endif
            co_return RedisResult<void>{};
        }

        co_return std::unexpected(RedisError{RedisErrorCategory::Io, "connect retry failed"});
    }

    task::Awaitable<RedisResult<void> > RedisClient::auth_and_select_unlocked() {
        normalize_auth(config_.username);
        normalize_auth(config_.password);

        if (config_.password.has_value()) {
            if (config_.username.has_value()) {
                std::string u = *config_.username;
                std::string p = *config_.password;
                std::string_view args_arr[2] = {u, p};

                auto r = co_await send_and_read_unlocked("AUTH", std::span<const std::string_view>(args_arr, 2));
                if (!r) co_return std::unexpected(r.error());
            } else {
                std::string p = *config_.password;
                std::string_view args_arr[1] = {p};

                auto r = co_await send_and_read_unlocked("AUTH", std::span<const std::string_view>(args_arr, 1));
                if (!r) co_return std::unexpected(r.error());
            }
        }

        if (config_.db != 0) {
            std::string db = std::to_string(config_.db);
            std::string_view args_arr[1] = {db};

            auto r = co_await send_and_read_unlocked("SELECT", std::span<const std::string_view>(args_arr, 1));
            if (!r) co_return std::unexpected(r.error());
        }

        co_return RedisResult<void>{};
    }

    std::vector<std::uint8_t>
    RedisClient::encode_command(std::string_view cmd, std::span<const std::string_view> args) {
        std::vector<std::uint8_t> out;

        auto append_sv = [&out](std::string_view s) {
            out.insert(out.end(),
                       reinterpret_cast<const std::uint8_t *>(s.data()),
                       reinterpret_cast<const std::uint8_t *>(s.data()) + s.size());
        };

        auto append_bulk = [&](std::string_view s) {
            append_sv("$");
            append_sv(std::to_string(s.size()));
            append_sv("\r\n");
            append_sv(s);
            append_sv("\r\n");
        };

        const std::size_t argc = 1 + args.size();
        append_sv("*");
        append_sv(std::to_string(argc));
        append_sv("\r\n");

        append_bulk(cmd);
        for (auto a: args) append_bulk(a);

        return out;
    }

    static inline std::int64_t monotonic_now_ms() noexcept {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    // Returns the per-IO timeout to use given an optional command deadline.
    // out: timeout in ms to pass to socket update_timeout (>= 1)
    // returns false iff the deadline has already expired (caller must bail with Timeout)
    static inline bool compute_io_timeout(std::int64_t deadline_ms, int io_timeout_ms, int &out) noexcept {
        if (deadline_ms <= 0) {
            out = io_timeout_ms;
            return true;
        }
        const auto rem = deadline_ms - monotonic_now_ms();
        if (rem <= 0) return false;
        std::int64_t cap = io_timeout_ms > 0 ? static_cast<std::int64_t>(io_timeout_ms) : rem;
        std::int64_t t = rem < cap ? rem : cap;
        if (t < 1) t = 1;
        out = static_cast<int>(t);
        return true;
    }

    task::Awaitable<RedisResult<RedisValue> > RedisClient::read_one_reply_unlocked(std::int64_t deadline_ms) {
        if (!socket_)
            co_return std::unexpected(RedisError{RedisErrorCategory::Io, "socket is null"});

        DynamicBuffer buf;
        buf.reserve(64 * 1024);

        for (;;) {
            if (auto v = parser_.next()) {
                RedisValue out = std::move(*v);
                if (out.type == RedisType::Error)
                    co_return std::unexpected(RedisError{RedisErrorCategory::ServerReply, out.as_string()});
                co_return out;
            }

            buf.clear();

            int io_to = 0;
            if (!compute_io_timeout(deadline_ms, config_.io_timeout_ms, io_to)) {
                hard_close_socket_unlocked();
                co_return std::unexpected(RedisError{RedisErrorCategory::Timeout, "command timeout (read deadline)"});
            }
            socket_->update_timeout(io_to);

            constexpr std::size_t max_read = 64 * 1024;
            const ssize_t rdsz = co_await socket_->async_read(buf, max_read);

#ifdef UREDIS_LOGS
            ulog::debug("RedisClient::read: this={} socket={} rdsz={}",
                        ptr_id(this), ptr_id(socket_.get()), rdsz);
#endif

            if (rdsz <= 0) {
                const bool timed_out = deadline_ms > 0 && monotonic_now_ms() >= deadline_ms;
                hard_close_socket_unlocked();
                if (timed_out)
                    co_return std::unexpected(RedisError{RedisErrorCategory::Timeout, "command timeout (read)"});
                co_return std::unexpected(RedisError{RedisErrorCategory::Io, "connection closed"});
            }

            parser_.feed(reinterpret_cast<const std::uint8_t *>(buf.data()), static_cast<std::size_t>(rdsz));
        }
    }

    task::Awaitable<RedisResult<RedisValue> > RedisClient::send_and_read_unlocked(
        std::string_view cmd,
        std::span<const std::string_view> args,
        std::int64_t deadline_ms) {
        if (!connected_ || closing_)
            co_return std::unexpected(RedisError{RedisErrorCategory::Io, "not connected"});
        if (!socket_)
            co_return std::unexpected(RedisError{RedisErrorCategory::Io, "socket is null"});

        std::vector<std::uint8_t> frame = encode_command(cmd, args);

        std::size_t off = 0;
        while (off < frame.size()) {
            int io_to = 0;
            if (!compute_io_timeout(deadline_ms, config_.io_timeout_ms, io_to)) {
                hard_close_socket_unlocked();
                co_return std::unexpected(RedisError{RedisErrorCategory::Timeout, "command timeout (write deadline)"});
            }
            socket_->update_timeout(io_to);

            const ssize_t n = co_await socket_->async_write(frame.data() + off, frame.size() - off);

#ifdef UREDIS_LOGS
            ulog::debug("RedisClient::write: this={} cmd=\"{}\" n={} off={} total={}",
                        ptr_id(this), std::string(cmd), n, off, frame.size());
#endif

            if (n <= 0) {
                const bool timed_out = deadline_ms > 0 && monotonic_now_ms() >= deadline_ms;
                hard_close_socket_unlocked();
                if (timed_out)
                    co_return std::unexpected(RedisError{RedisErrorCategory::Timeout, "command timeout (write)"});
                co_return std::unexpected(RedisError{RedisErrorCategory::Io, "write failed"});
            }
            off += static_cast<std::size_t>(n);
        }

        co_return co_await read_one_reply_unlocked(deadline_ms);
    }

    task::Awaitable<RedisResult<RedisValue> > RedisClient::command(
        std::string_view cmd,
        std::span<const std::string_view> args) {
        co_return co_await command_impl(cmd, args, config_.command_timeout_ms);
    }

    task::Awaitable<RedisResult<RedisValue> > RedisClient::command_timed(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms) {
        co_return co_await command_impl(cmd, args, timeout_ms);
    }

    task::Awaitable<RedisResult<RedisValue> > RedisClient::command_impl(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms) {
        if (!connected_ || closing_ || !socket_) {
            auto c = co_await connect_unlocked();
            if (!c) co_return std::unexpected(c.error());
        }

        // Compute monotonic deadline once for the whole command (covers both
        // attempts when a transient IO error triggers a reconnect+retry).
        // timeout_ms <= 0 disables the deadline (legacy per-IO behavior).
        const std::int64_t deadline_ms = (timeout_ms > 0)
                                             ? (monotonic_now_ms() + static_cast<std::int64_t>(timeout_ms))
                                             : 0;

        for (int attempt = 0; attempt < 2; ++attempt) {
#ifdef UREDIS_LOGS
            ulog::debug("RedisClient::command: attempt={} this={} cmd=\"{}\" argc={} socket={} deadline_ms={}",
                        attempt, ptr_id(this), std::string(cmd), args.size(), ptr_id(socket_.get()),
                        static_cast<long long>(deadline_ms));
#endif

            // Bail out early if the user budget is already gone.
            if (deadline_ms > 0 && monotonic_now_ms() >= deadline_ms) {
                hard_close_socket_unlocked();
                co_return std::unexpected(RedisError{RedisErrorCategory::Timeout, "command timeout"});
            }

            auto r = co_await send_and_read_unlocked(cmd, args, deadline_ms);
            if (r) co_return r;

            const auto &e = r.error();

            // Never retry on Timeout: the caller's budget is the contract.
            if (e.category == RedisErrorCategory::Timeout)
                co_return std::unexpected(e);

            if (e.category == RedisErrorCategory::Io && attempt == 0) {
#ifdef UREDIS_LOGS
                ulog::warn("RedisClient::command: io error, reconnect+retry: this={} msg=\"{}\"",
                           ptr_id(this), e.message);
#endif
                hard_close_socket_unlocked();

                auto c = co_await connect_unlocked();
                if (!c) co_return std::unexpected(c.error());
                continue;
            }

            co_return std::unexpected(e);
        }

        co_return std::unexpected(RedisError{RedisErrorCategory::Io, "retry failed"});
    }

    task::Awaitable<RedisResult<std::optional<std::string> > > RedisClient::get(std::string_view key) {
        std::string_view a[1] = {key};
        auto r = co_await command("GET", std::span<const std::string_view>(a, 1));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type == RedisType::Null) co_return std::optional<std::string>{};
        if (v.type != RedisType::BulkString && v.type != RedisType::SimpleString)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "GET: unexpected type"});
        co_return std::optional<std::string>{v.as_string()};
    }

    task::Awaitable<RedisResult<void> > RedisClient::set(std::string_view key, std::string_view value) {
        std::string_view a[2] = {key, value};
        auto r = co_await command("SET", std::span<const std::string_view>(a, 2));
        if (!r) co_return std::unexpected(r.error());
        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void> > RedisClient::setex(std::string_view key, int ttl_sec, std::string_view value) {
        std::string ttl = std::to_string(ttl_sec);
        std::string_view a[3] = {key, ttl, value};
        auto r = co_await command("SETEX", std::span<const std::string_view>(a, 3));
        if (!r) co_return std::unexpected(r.error());
        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::del(std::span<const std::string_view> keys) {
        if (keys.empty()) co_return int64_t{0};
        auto r = co_await command("DEL", keys);
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "DEL: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::incrby(std::string_view key, int64_t delta) {
        std::string d = std::to_string(delta);
        std::string_view a[2] = {key, d};
        auto r = co_await command("INCRBY", std::span<const std::string_view>(a, 2));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "INCRBY: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::hset(std::string_view key, std::string_view field,
                                                             std::string_view value) {
        std::string_view a[3] = {key, field, value};
        auto r = co_await command("HSET", std::span<const std::string_view>(a, 3));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "HSET: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<std::optional<std::string> > > RedisClient::hget(
        std::string_view key, std::string_view field) {
        std::string_view a[2] = {key, field};
        auto r = co_await command("HGET", std::span<const std::string_view>(a, 2));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type == RedisType::Null) co_return std::optional<std::string>{};
        if (v.type != RedisType::BulkString && v.type != RedisType::SimpleString)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "HGET: unexpected type"});
        co_return std::optional<std::string>{v.as_string()};
    }

    task::Awaitable<RedisResult<std::unordered_map<std::string, std::string> > > RedisClient::hgetall(
        std::string_view key) {
        std::string_view a[1] = {key};
        auto r = co_await command("HGETALL", std::span<const std::string_view>(a, 1));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type == RedisType::Null) co_return std::unordered_map<std::string, std::string>{};
        if (v.type != RedisType::Array)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "HGETALL: unexpected type"});

        const auto &arr = v.as_array();
        if ((arr.size() & 1u) != 0u)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "HGETALL: odd array size"});

        std::unordered_map<std::string, std::string> out;
        out.reserve(arr.size() / 2);

        for (size_t i = 0; i < arr.size(); i += 2) {
            const auto &f = arr[i];
            const auto &val = arr[i + 1];
            if (!f.is_bulk_string() && !f.is_simple_string()) continue;
            if (!val.is_bulk_string() && !val.is_simple_string()) continue;
            out.emplace(f.as_string(), val.as_string());
        }

        co_return out;
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::sadd(std::string_view key,
                                                             std::span<const std::string_view> members) {
        if (members.empty()) co_return int64_t{0};

        std::vector<std::string_view> a;
        a.reserve(1 + members.size());
        a.push_back(key);
        for (auto m: members) a.push_back(m);

        auto r = co_await command("SADD", std::span<const std::string_view>(a.data(), a.size()));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "SADD: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::srem(std::string_view key,
                                                             std::span<const std::string_view> members) {
        if (members.empty()) co_return int64_t{0};

        std::vector<std::string_view> a;
        a.reserve(1 + members.size());
        a.push_back(key);
        for (auto m: members) a.push_back(m);

        auto r = co_await command("SREM", std::span<const std::string_view>(a.data(), a.size()));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "SREM: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<std::vector<std::string> > > RedisClient::smembers(std::string_view key) {
        std::string_view a[1] = {key};
        auto r = co_await command("SMEMBERS", std::span<const std::string_view>(a, 1));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type == RedisType::Null) co_return std::vector<std::string>{};
        if (v.type != RedisType::Array)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "SMEMBERS: unexpected type"});

        const auto &arr = v.as_array();
        std::vector<std::string> out;
        out.reserve(arr.size());

        for (const auto &it: arr) {
            if (!it.is_bulk_string() && !it.is_simple_string()) continue;
            out.push_back(it.as_string());
        }

        co_return out;
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::lpush(std::string_view key,
                                                              std::span<const std::string_view> values) {
        if (values.empty()) co_return int64_t{0};

        std::vector<std::string_view> a;
        a.reserve(1 + values.size());
        a.push_back(key);
        for (auto v: values) a.push_back(v);

        auto r = co_await command("LPUSH", std::span<const std::string_view>(a.data(), a.size()));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "LPUSH: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<std::vector<std::string> > > RedisClient::lrange(
        std::string_view key, int64_t start, int64_t stop) {
        std::string s1 = std::to_string(start);
        std::string s2 = std::to_string(stop);
        std::string_view a[3] = {key, s1, s2};

        auto r = co_await command("LRANGE", std::span<const std::string_view>(a, 3));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type != RedisType::Array)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "LRANGE: unexpected type"});

        const auto &arr = v.as_array();
        std::vector<std::string> out;
        out.reserve(arr.size());

        for (const auto &it: arr) {
            if (!it.is_bulk_string() && !it.is_simple_string()) continue;
            out.push_back(it.as_string());
        }

        co_return out;
    }

    task::Awaitable<RedisResult<int64_t> > RedisClient::zadd(
        std::string_view key,
        std::span<const std::pair<std::string, double>> members) {
        if (members.empty()) co_return int64_t{0};

        std::vector<std::string> scores;
        scores.reserve(members.size());

        std::vector<std::string_view> a;
        a.reserve(1 + members.size() * 2);
        a.push_back(key);

        for (const auto &m: members) {
            scores.push_back(std::to_string(m.second));
            a.push_back(scores.back());
            a.push_back(m.first);
        }

        auto r = co_await command("ZADD", std::span<const std::string_view>(a.data(), a.size()));
        if (!r) co_return std::unexpected(r.error());
        if (r->type != RedisType::Integer)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "ZADD: unexpected type"});
        co_return r->as_integer();
    }

    task::Awaitable<RedisResult<std::vector<std::pair<std::string, double> > > > RedisClient::zrange_with_scores(
        std::string_view key, int64_t start, int64_t stop) {
        std::string s1 = std::to_string(start);
        std::string s2 = std::to_string(stop);
        std::string with = "WITHSCORES";
        std::string_view a[4] = {key, s1, s2, with};

        auto r = co_await command("ZRANGE", std::span<const std::string_view>(a, 4));
        if (!r) co_return std::unexpected(r.error());

        const RedisValue &v = *r;
        if (v.type != RedisType::Array)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "ZRANGE: unexpected type"});

        const auto &arr = v.as_array();
        if ((arr.size() & 1u) != 0u)
            co_return std::unexpected(RedisError{RedisErrorCategory::Protocol, "ZRANGE: odd array size"});

        std::vector<std::pair<std::string, double> > out;
        out.reserve(arr.size() / 2);

        for (size_t i = 0; i < arr.size(); i += 2) {
            const auto &m = arr[i];
            const auto &sc = arr[i + 1];
            if (!m.is_bulk_string() && !m.is_simple_string()) continue;
            if (!sc.is_bulk_string() && !sc.is_simple_string()) continue;
            out.emplace_back(m.as_string(), std::stod(sc.as_string()));
        }

        co_return out;
    }
} // namespace usub::uredis
