#ifndef UREDIS_REDISSENTINEL_H
#define UREDIS_REDISSENTINEL_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>

#include "uredis/RedisClient.h"

namespace usub::uredis
{
    namespace task = usub::uvent::task;

    struct RedisSentinelNode
    {
        std::string host;
        std::uint16_t port{26379};

        std::optional<std::string> username;
        std::optional<std::string> password;
    };

    struct RedisSentinelConfig
    {
        std::string master_name;

        std::vector<RedisSentinelNode> sentinels;

        int connect_timeout_ms{3000};
        int io_timeout_ms{3000};

        // The resolved master inherits base_redis (including base_redis.tls).
        RedisConfig base_redis{};

        // TLS for the connections to the sentinels themselves; disabled by default.
        RedisTlsConfig sentinel_tls{};
    };

    task::Awaitable<RedisResult<RedisConfig>>
    resolve_master_from_sentinel(const RedisSentinelConfig& cfg);
}

#endif