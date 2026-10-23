#include "uredis/RedisSubscriber.h"

#include <algorithm>
#include <chrono>

#include <charconv>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    using usub::uvent::utils::DynamicBuffer;

    RedisSubscriber::RedisSubscriber(RedisConfig cfg)
        : config_(std::move(cfg))
    {
    }

    std::vector<uint8_t> RedisSubscriber::encode_command(
        std::string_view cmd,
        std::span<const std::string_view> args)
    {
        std::vector<uint8_t> out;
        std::size_t argc = 1 + args.size();
        out.reserve(64 + cmd.size() + args.size() * 16);

        auto append_str = [&out](std::string_view s)
        {
            out.insert(out.end(),
                       reinterpret_cast<const uint8_t*>(s.data()),
                       reinterpret_cast<const uint8_t*>(s.data()) + s.size());
        };

        auto append_bulk = [&append_str](std::string_view s, std::vector<uint8_t>&)
        {
            std::string len = std::to_string(s.size());
            std::string prefix = "$" + len + "\r\n";
            append_str(prefix);
            append_str(s);
            append_str("\r\n");
        };

        {
            std::string header = "*" + std::to_string(argc) + "\r\n";
            append_str(header);
        }

        append_bulk(cmd, out);
        for (auto& a : args) append_bulk(a, out);
        return out;
    }

    task::Awaitable<RedisResult<void>> RedisSubscriber::connect()
    {
        if (this->connected_)
        {
            co_return RedisResult<void>{};
        }

        std::string port_str = std::to_string(this->config_.port);

#ifdef UREDIS_LOGS
        ulog::info("RedisSubscriber::connect: host={} port={}",
                     this->config_.host,
                     this->config_.port);
#endif

        auto res = co_await this->socket_.async_connect(this->config_.host.c_str(),
                                                        port_str.c_str(),
                                                        std::chrono::milliseconds{
                                                            std::max(0, this->config_.connect_timeout_ms)});
        if (res.has_value())
        {
            RedisError err{RedisErrorCategory::Io, "async_connect failed"};
            co_return std::unexpected(err);
        }

        this->socket_.set_timeout_ms(this->config_.io_timeout_ms);

        if (this->config_.tls.enabled)
        {
            auto session = detail::TlsSession::create(this->config_.tls, this->config_.host);
            if (!session)
            {
                this->socket_.shutdown();
                co_return std::unexpected(session.error());
            }
            this->tls_ = std::move(*session);
            auto hs = co_await this->tls_->handshake(this->socket_, this->config_.io_timeout_ms > 0
                                                                        ? this->config_.io_timeout_ms
                                                                        : 5000);
            if (!hs)
            {
                this->socket_.shutdown();
                this->tls_.reset();
                co_return std::unexpected(hs.error());
            }
        }

        this->connected_ = true;
        this->closing_ = false;

        system::co_spawn(this->reader_loop());

        if (this->config_.password.has_value())
        {
            std::string user;
            std::string pass = *this->config_.password;

            std::array<std::string_view, 2> arr2{};
            std::array<std::string_view, 1> arr1{};
            std::span<const std::string_view> span_args;

            if (this->config_.username.has_value())
            {
                user = *this->config_.username;
                arr2[0] = user;
                arr2[1] = pass;
                span_args = std::span<const std::string_view>(arr2.data(), 2);
            }
            else
            {
                arr1[0] = pass;
                span_args = std::span<const std::string_view>(arr1.data(), 1);
            }

            auto frame = encode_command("AUTH", span_args);
            {
                auto w = co_await this->write_mutex_.lock();
                auto wrsz = co_await this->write_frame(frame);
                this->socket_.update_timeout(this->config_.io_timeout_ms);
                if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
                {
                    RedisError err{RedisErrorCategory::Io, "AUTH write failed"};
                    this->fail_all(RedisErrorCategory::Io, "auth write failed");
                    co_return std::unexpected(err);
                }
            }
        }

        if (this->config_.db != 0)
        {
            std::string db_str = std::to_string(this->config_.db);
            std::string_view args_arr[1] = {db_str};
            auto frame = encode_command("SELECT",
                                        std::span<const std::string_view>(args_arr, 1));

            auto w = co_await this->write_mutex_.lock();
            auto wrsz = co_await this->write_frame(frame);
            this->socket_.update_timeout(this->config_.io_timeout_ms);
            if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
            {
                RedisError err{RedisErrorCategory::Io, "SELECT write failed"};
                this->fail_all(RedisErrorCategory::Io, "select write failed");
                co_return std::unexpected(err);
            }
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisSubscriber::subscribe(std::string channel, MessageCallback cb)
    {
        if (!this->connected_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisSubscriber not connected"};
            co_return std::unexpected(err);
        }

        auto st = std::make_shared<PendingSub>();
        st->callback = std::move(cb);
        std::string key = channel;

        this->pending_sub_.emplace(key, st);

        std::string_view args_arr[1] = {key};
        auto frame = encode_command("SUBSCRIBE",
                                    std::span<const std::string_view>(args_arr, 1));

        {
            auto w = co_await this->write_mutex_.lock();
            auto wrsz = co_await this->write_frame(frame);
            this->socket_.update_timeout(this->config_.io_timeout_ms);
            if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
            {
                this->pending_sub_.erase(key);
                RedisError err{RedisErrorCategory::Io, "SUBSCRIBE write failed"};
                co_return std::unexpected(err);
            }
        }

        co_await st->event.wait();
        co_return st->result;
    }

    task::Awaitable<RedisResult<void>> RedisSubscriber::psubscribe(std::string pattern, MessageCallback cb)
    {
        if (!this->connected_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisSubscriber not connected"};
            co_return std::unexpected(err);
        }

        auto st = std::make_shared<PendingSub>();
        st->callback = std::move(cb);
        std::string key = pattern;

        this->pending_psub_.emplace(key, st);

        std::string_view args_arr[1] = {key};
        auto frame = encode_command("PSUBSCRIBE",
                                    std::span<const std::string_view>(args_arr, 1));

        {
            auto w = co_await this->write_mutex_.lock();
            auto wrsz = co_await this->write_frame(frame);
            this->socket_.update_timeout(this->config_.io_timeout_ms);
            if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
            {
                this->pending_psub_.erase(key);
                RedisError err{RedisErrorCategory::Io, "PSUBSCRIBE write failed"};
                co_return std::unexpected(err);
            }
        }

        co_await st->event.wait();
        co_return st->result;
    }

    task::Awaitable<RedisResult<void>> RedisSubscriber::unsubscribe(std::string channel)
    {
        if (!this->connected_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisSubscriber not connected"};
            co_return std::unexpected(err);
        }

        auto st = std::make_shared<PendingUnsub>();
        std::string key = channel;
        this->pending_unsub_.emplace(key, st);

        std::string_view args_arr[1] = {key};
        auto frame = encode_command("UNSUBSCRIBE",
                                    std::span<const std::string_view>(args_arr, 1));

        {
            auto w = co_await this->write_mutex_.lock();
            auto wrsz = co_await this->write_frame(frame);
            this->socket_.update_timeout(this->config_.io_timeout_ms);
            if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
            {
                this->pending_unsub_.erase(key);
                RedisError err{RedisErrorCategory::Io, "UNSUBSCRIBE write failed"};
                co_return std::unexpected(err);
            }
        }

        co_await st->event.wait();
        co_return st->result;
    }

    task::Awaitable<RedisResult<void>> RedisSubscriber::punsubscribe(std::string pattern)
    {
        if (!this->connected_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisSubscriber not connected"};
            co_return std::unexpected(err);
        }

        auto st = std::make_shared<PendingUnsub>();
        std::string key = pattern;
        this->pending_punsub_.emplace(key, st);

        std::string_view args_arr[1] = {key};
        auto frame = encode_command("PUNSUBSCRIBE",
                                    std::span<const std::string_view>(args_arr, 1));

        {
            auto w = co_await this->write_mutex_.lock();
            auto wrsz = co_await this->write_frame(frame);
            this->socket_.update_timeout(this->config_.io_timeout_ms);
            if (wrsz <= 0 || static_cast<std::size_t>(wrsz) != frame.size())
            {
                this->pending_punsub_.erase(key);
                RedisError err{RedisErrorCategory::Io, "PUNSUBSCRIBE write failed"};
                co_return std::unexpected(err);
            }
        }

        co_await st->event.wait();
        co_return st->result;
    }

    task::Awaitable<void> RedisSubscriber::close()
    {
        this->closing_ = true;
        this->connected_ = false;
        this->socket_.shutdown();
        co_return;
    }

    void RedisSubscriber::fail_all(RedisErrorCategory cat, std::string_view msg)
    {
        RedisError err{cat, std::string(msg)};

        for (auto& [_, st] : this->pending_sub_)
        {
            st->result = std::unexpected(err);
            st->event.set();
        }
        for (auto& [_, st] : this->pending_psub_)
        {
            st->result = std::unexpected(err);
            st->event.set();
        }
        for (auto& [_, st] : this->pending_unsub_)
        {
            st->result = std::unexpected(err);
            st->event.set();
        }
        for (auto& [_, st] : this->pending_punsub_)
        {
            st->result = std::unexpected(err);
            st->event.set();
        }

        this->pending_sub_.clear();
        this->pending_psub_.clear();
        this->pending_unsub_.clear();
        this->pending_punsub_.clear();
    }

    void RedisSubscriber::handle_array(RedisValue&& v)
    {
        if (v.type != RedisType::Array) return;
        const auto& arr = v.as_array();
        if (arr.empty()) return;
        if (!arr[0].is_bulk_string() && !arr[0].is_simple_string()) return;

        const std::string& kind = arr[0].as_string();

        if (kind == "message")
        {
            if (arr.size() < 3) return;
            const std::string& channel = arr[1].as_string();
            const std::string& payload = arr[2].as_string();

            auto it = this->channel_handlers_.find(channel);
            if (it != this->channel_handlers_.end() && it->second)
            {
                it->second(channel, payload);
            }
            return;
        }

        if (kind == "pmessage")
        {
            if (arr.size() < 4) return;
            const std::string& pattern = arr[1].as_string();
            const std::string& channel = arr[2].as_string();
            const std::string& payload = arr[3].as_string();

            auto it = this->pattern_handlers_.find(pattern);
            if (it != this->pattern_handlers_.end() && it->second)
            {
                it->second(channel, payload);
            }
            return;
        }

        if (kind == "subscribe")
        {
            if (arr.size() < 2) return;
            const std::string& channel = arr[1].as_string();
            auto it = this->pending_sub_.find(channel);
            if (it != this->pending_sub_.end())
            {
                auto st = it->second;
                this->channel_handlers_[channel] = st->callback;
                st->result = RedisResult<void>{};
                st->event.set();
                this->pending_sub_.erase(it);
            }
            return;
        }

        if (kind == "psubscribe")
        {
            if (arr.size() < 2) return;
            const std::string& pattern = arr[1].as_string();
            auto it = this->pending_psub_.find(pattern);
            if (it != this->pending_psub_.end())
            {
                auto st = it->second;
                this->pattern_handlers_[pattern] = st->callback;
                st->result = RedisResult<void>{};
                st->event.set();
                this->pending_psub_.erase(it);
            }
            return;
        }

        if (kind == "unsubscribe")
        {
            if (arr.size() < 2) return;
            const std::string& channel = arr[1].as_string();
            this->channel_handlers_.erase(channel);
            auto it = this->pending_unsub_.find(channel);
            if (it != this->pending_unsub_.end())
            {
                auto st = it->second;
                st->result = RedisResult<void>{};
                st->event.set();
                this->pending_unsub_.erase(it);
            }
            return;
        }

        if (kind == "punsubscribe")
        {
            if (arr.size() < 2) return;
            const std::string& pattern = arr[1].as_string();
            this->pattern_handlers_.erase(pattern);
            auto it = this->pending_punsub_.find(pattern);
            if (it != this->pending_punsub_.end())
            {
                auto st = it->second;
                st->result = RedisResult<void>{};
                st->event.set();
                this->pending_punsub_.erase(it);
            }
            return;
        }
    }

    task::Awaitable<ssize_t> RedisSubscriber::write_frame(std::vector<std::uint8_t>& frame)
    {
        if (this->tls_)
        {
            auto tls = this->tls_;
            co_return co_await tls->write_all(this->socket_, frame.data(), frame.size(), this->config_.io_timeout_ms);
        }
        co_return co_await this->socket_.async_write(frame.data(), frame.size());
    }

    task::Awaitable<void> RedisSubscriber::reader_loop()
    {
#ifdef UREDIS_LOGS
        ulog::info("RedisSubscriber::reader_loop: start");
#endif
        static constexpr std::size_t max_read_size = 64 * 1024;
        DynamicBuffer buf;
        buf.reserve(max_read_size);
        std::vector<std::uint8_t> plain(this->tls_ ? max_read_size : 0); // TLS only: decrypted bytes

        while (!this->closing_)
        {
            buf.clear();
            ssize_t rdsz = 0;
            if (this->tls_)
            {
                auto tls = this->tls_;
                rdsz = co_await tls->read_some(this->socket_, plain.data(), plain.size(), this->config_.io_timeout_ms);
            }
            else
            {
                rdsz = co_await this->socket_.async_read(buf, max_read_size);
            }
            this->socket_.update_timeout(this->config_.io_timeout_ms);

            if (rdsz <= 0)
            {
#ifdef UREDIS_LOGS
                ulog::info("RedisSubscriber::reader_loop: connection closed, rdsz={}", rdsz);
#endif
                break;
            }

            if (this->tls_)
                this->parser_.feed(plain.data(), static_cast<std::size_t>(rdsz));
            else
                this->parser_.feed(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());

            while (true)
            {
                auto val_opt = this->parser_.next();
                if (!val_opt) break;

                RedisValue v = std::move(*val_opt);

                if (v.type == RedisType::Array)
                {
                    this->handle_array(std::move(v));
                }
                else if (v.type == RedisType::Error)
                {
#ifdef UREDIS_LOGS
                    ulog::error("RedisSubscriber::reader_loop: server error: {}",
                                  v.as_string());
#endif
                }
            }
        }

        this->closing_ = true;
        this->connected_ = false;
        this->socket_.shutdown();

        this->fail_all(RedisErrorCategory::Io, "subscriber connection closed");

#ifdef UREDIS_LOGS
        ulog::info("RedisSubscriber::reader_loop: stop");
#endif
        co_return;
    }
} // namespace usub::uredis
