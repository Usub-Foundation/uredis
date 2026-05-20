#include "uredis/RedisSentinelPool.h"

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    namespace task = usub::uvent::task;

    RedisSentinelPool::RedisSentinelPool(RedisSentinelConfig cfg)
        : cfg_(std::move(cfg))
    {}

    task::Awaitable<RedisResult<void>>
    RedisSentinelPool::ensure_connected_locked()
    {
        if (connected_ && master_)
            co_return RedisResult<void>{};

        auto master_cfg_res = co_await resolve_master_from_sentinel(cfg_);
        if (!master_cfg_res)
            co_return std::unexpected(master_cfg_res.error());

        auto master_cfg = master_cfg_res.value();

        auto client = std::make_shared<RedisClient>(master_cfg);
        auto c = co_await client->connect();
        if (!c)
            co_return std::unexpected(c.error());

#ifdef UREDIS_LOGS
        usub::ulog::info("RedisSentinelPool: connected to master {}:{}",
                         master_cfg.host, master_cfg.port);
#endif

        master_ = std::move(client);
        connected_ = true;

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>>
    RedisSentinelPool::connect()
    {
        auto guard = co_await mutex_.lock();
        co_return co_await ensure_connected_locked();
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisSentinelPool::get_master_client()
    {
        auto guard = co_await mutex_.lock();

        auto ec = co_await ensure_connected_locked();
        if (!ec)
            co_return std::unexpected(ec.error());

        co_return master_;
    }

    task::Awaitable<RedisResult<RedisValue>>
    RedisSentinelPool::command(std::string_view cmd,
                               std::span<const std::string_view> args)
    {
        std::shared_ptr<RedisClient> client;

        {
            auto guard = co_await mutex_.lock();

            auto ec = co_await ensure_connected_locked();
            if (!ec)
                co_return std::unexpected(ec.error());

            client = master_;
        }

        auto res = co_await client->command(cmd, args);
        if (res)
            co_return res;

        auto err = res.error();

        // Timeout: contract is "always exit promptly" — do NOT retry. Drop
        // the cached master so the next user command re-resolves through
        // sentinels (the master may have failed over, which is exactly the
        // kind of thing that causes timeouts).
        if (err.category == RedisErrorCategory::Timeout)
        {
            auto guard = co_await mutex_.lock();
            connected_ = false;
            master_.reset();
            co_return std::unexpected(err);
        }

        if (err.category != RedisErrorCategory::Io)
            co_return std::unexpected(err);

#ifdef UREDIS_LOGS
        usub::ulog::warn("RedisSentinelPool: IO error '{}', trying to re-resolve master",
                         err.message);
#endif

        {
            auto guard = co_await mutex_.lock();
            connected_ = false;
            master_.reset();
            auto ec = co_await ensure_connected_locked();
            if (!ec)
                co_return std::unexpected(err);

            client = master_;
        }

        co_return co_await client->command(cmd, args);
    }

    task::Awaitable<RedisResult<RedisValue>>
    RedisSentinelPool::command_timed(std::string_view cmd,
                                     std::span<const std::string_view> args,
                                     int timeout_ms)
    {
        // Per-call deadline: cover the master-resolve + IO portion under the
        // same budget. We don't retry on Timeout (would exceed the budget).
        std::shared_ptr<RedisClient> client;

        {
            auto guard = co_await mutex_.lock();

            auto ec = co_await ensure_connected_locked();
            if (!ec)
                co_return std::unexpected(ec.error());

            client = master_;
        }

        auto res = co_await client->command_timed(cmd, args, timeout_ms);
        if (res)
            co_return res;

        auto err = res.error();

        // On Timeout or IO error, drop the cached master so subsequent calls
        // re-resolve. On Timeout we do NOT retry within the same call.
        if (err.category == RedisErrorCategory::Timeout
            || err.category == RedisErrorCategory::Io)
        {
            auto guard = co_await mutex_.lock();
            connected_ = false;
            master_.reset();
        }

        co_return std::unexpected(err);
    }
}