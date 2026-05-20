#ifndef REDISPOOL_H
#define REDISPOOL_H

#include <atomic>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <array>

#include "uredis/RedisClient.h"

namespace usub::uredis
{
    namespace task = usub::uvent::task;

    struct RedisPoolConfig
    {
        std::string host;
        std::uint16_t port{6379};
        int db{0};

        std::optional<std::string> username;
        std::optional<std::string> password;

        std::size_t size{4};

        int connect_timeout_ms{5000};
        int io_timeout_ms{5000};

        // Optional total budget for a single command, forwarded to each
        // underlying RedisClient. 0 = disabled (legacy behavior).
        int command_timeout_ms{0};
    };

    class RedisPool
    {
    public:
        explicit RedisPool(RedisPoolConfig cfg);

        task::Awaitable<RedisResult<void>> connect_all();

        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template <typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            Args&&... args)
        {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...};
            co_return co_await this->command(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()));
        }

        // Per-call timeout override, see RedisClient::command_timed.
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            std::string_view cmd,
            std::span<const std::string_view> args,
            int timeout_ms);

        template <typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            int timeout_ms,
            std::string_view cmd,
            Args&&... args)
        {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...};
            co_return co_await this->command_timed(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()),
                timeout_ms);
        }

    private:
        RedisPoolConfig cfg_;
        std::vector<std::shared_ptr<RedisClient>> clients_;
        std::atomic<std::size_t> rr_{0};
    };
} // namespace usub::uredis

#endif //REDISPOOL_H
