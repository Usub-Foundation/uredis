#include "uredis/RedisPool.h"

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    RedisPool::RedisPool(RedisPoolConfig cfg)
        : cfg_(std::move(cfg))
    {
        if (this->cfg_.size == 0) this->cfg_.size = 1;

        this->clients_.reserve(this->cfg_.size);
        for (std::size_t i = 0; i < this->cfg_.size; ++i)
        {
            RedisConfig rc;
            rc.host = this->cfg_.host;
            rc.port = this->cfg_.port;
            rc.db = this->cfg_.db;
            rc.username = this->cfg_.username;
            rc.password = this->cfg_.password;
            rc.connect_timeout_ms = this->cfg_.connect_timeout_ms;
            rc.io_timeout_ms = this->cfg_.io_timeout_ms;
            rc.command_timeout_ms = this->cfg_.command_timeout_ms;

            this->clients_.push_back(std::make_shared<RedisClient>(rc));
        }
    }

    task::Awaitable<RedisResult<void>> RedisPool::connect_all()
    {
        for (auto& c : this->clients_)
        {
            auto res = co_await c->connect();
            if (!res)
            {
#ifdef UREDIS_LOGS
                ulog::error("RedisPool::connect_all: connect failed: {}", res.error().message);
#endif
                co_return std::unexpected(res.error());
            }
        }
        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<RedisValue>> RedisPool::command(
        std::string_view cmd,
        std::span<const std::string_view> args)
    {
        if (this->clients_.empty())
        {
            RedisError err{RedisErrorCategory::Io, "RedisPool has no clients"};
            co_return std::unexpected(err);
        }

        std::size_t idx = this->rr_.fetch_add(1, std::memory_order_relaxed) % this->clients_.size();
        auto& client = this->clients_[idx];

        co_return co_await client->command(cmd, args);
    }

    task::Awaitable<RedisResult<RedisValue>> RedisPool::command_timed(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms)
    {
        if (this->clients_.empty())
        {
            RedisError err{RedisErrorCategory::Io, "RedisPool has no clients"};
            co_return std::unexpected(err);
        }

        std::size_t idx = this->rr_.fetch_add(1, std::memory_order_relaxed) % this->clients_.size();
        auto& client = this->clients_[idx];

        co_return co_await client->command_timed(cmd, args, timeout_ms);
    }
} // namespace usub::uredis
