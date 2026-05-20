#ifndef UREDIS_REDISSENTINELPOOL_H
#define UREDIS_REDISSENTINELPOOL_H

#include <memory>
#include <string>
#include <span>

#include "uvent/sync/AsyncMutex.h"
#include "uredis/RedisClient.h"
#include "uredis/RedisSentinel.h"

namespace usub::uredis
{
    namespace task = usub::uvent::task;
    namespace sync = usub::uvent::sync;

    class RedisSentinelPool
    {
    public:
        explicit RedisSentinelPool(RedisSentinelConfig cfg);

        task::Awaitable<RedisResult<void>> connect();

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>> get_master_client();

        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            Args&&... args)
        {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()));
        }

        // Per-call timeout override. On expiration the call returns
        // RedisErrorCategory::Timeout, drops the cached master, and the next
        // command re-resolves the master through the sentinels — similar to
        // the cluster rebuild behavior on timeout.
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            std::string_view cmd,
            std::span<const std::string_view> args,
            int timeout_ms);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            int timeout_ms,
            std::string_view cmd,
            Args&&... args)
        {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command_timed(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()),
                timeout_ms);
        }

        const RedisSentinelConfig& config() const { return cfg_; }

    private:
        RedisSentinelConfig cfg_;

        std::shared_ptr<RedisClient> master_;
        bool connected_{false};

        sync::AsyncMutex mutex_;

        task::Awaitable<RedisResult<void>> ensure_connected_locked();
    };
}

#endif