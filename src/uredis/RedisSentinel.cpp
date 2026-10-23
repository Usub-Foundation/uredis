#include "uredis/RedisSentinel.h"

#include <charconv>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    namespace task   = usub::uvent::task;
    namespace system = usub::uvent::system;

    task::Awaitable<RedisResult<RedisConfig>> resolve_master_from_sentinel(
        const RedisSentinelConfig& cfg)
    {
        if (cfg.sentinels.empty())
        {
            RedisError err{RedisErrorCategory::Io, "RedisSentinel: no sentinels configured"};
            co_return std::unexpected(err);
        }

        for (const auto& node : cfg.sentinels)
        {
#ifdef UREDIS_LOGS
            usub::ulog::info(
                "RedisSentinel::resolve_master: try sentinel {}:{} (master_name={})",
                node.host,
                node.port,
                cfg.master_name);
#endif
            RedisConfig sent_cfg;
            sent_cfg.host = node.host;
            sent_cfg.port = node.port;
            sent_cfg.db   = 0;

            sent_cfg.username = node.username;
            sent_cfg.password = node.password;

            sent_cfg.connect_timeout_ms = cfg.connect_timeout_ms;
            sent_cfg.io_timeout_ms      = cfg.io_timeout_ms;
            sent_cfg.tls                = cfg.sentinel_tls;

            RedisClient sentinel_client{sent_cfg};
            auto c = co_await sentinel_client.connect();
            if (!c)
            {
#ifdef UREDIS_LOGS
                const auto& err = c.error();
                usub::ulog::warn(
                    "RedisSentinel::resolve_master: connect to sentinel {}:{} failed: {}",
                    node.host,
                    node.port,
                    err.message);
#endif
                continue;
            }

            std::string_view args_arr[2] = {"get-master-addr-by-name", cfg.master_name};
            auto resp = co_await sentinel_client.command(
                "SENTINEL",
                std::span<const std::string_view>(args_arr, 2));

            if (!resp)
            {
#ifdef UREDIS_LOGS
                const auto& err = resp.error();
                usub::ulog::warn(
                    "RedisSentinel::resolve_master: SENTINEL get-master-addr-by-name failed: {}",
                    err.message);
#endif
                continue;
            }

            const RedisValue& v = *resp;
            if (!v.is_array())
            {
#ifdef UREDIS_LOGS
                usub::ulog::warn(
                    "RedisSentinel::resolve_master: unexpected reply type (not array)");
#endif
                continue;
            }

            const auto& arr = v.as_array();
            if (arr.size() < 2
                || (!arr[0].is_bulk_string() && !arr[0].is_simple_string())
                || (!arr[1].is_bulk_string() && !arr[1].is_simple_string()))
            {
#ifdef UREDIS_LOGS
                usub::ulog::warn(
                    "RedisSentinel::resolve_master: unexpected array format (need [host, port])");
#endif
                continue;
            }

            const std::string& master_host     = arr[0].as_string();
            const std::string& master_port_str = arr[1].as_string();

            std::uint16_t master_port = 0;
            {
                unsigned long tmp{};
                auto* begin = master_port_str.data();
                auto* end   = master_port_str.data() + master_port_str.size();
                auto [p, ec] = std::from_chars(begin, end, tmp);
                if (ec != std::errc{} || tmp > 65535)
                {
#ifdef UREDIS_LOGS
                    usub::ulog::warn(
                        "RedisSentinel::resolve_master: invalid port '{}'",
                        master_port_str);
#endif
                    continue;
                }
                master_port = static_cast<std::uint16_t>(tmp);
            }

            RedisConfig master_cfg = cfg.base_redis;
            master_cfg.host = master_host;
            master_cfg.port = master_port;

#ifdef UREDIS_LOGS
            usub::ulog::info(
                "RedisSentinel::resolve_master: resolved master {}:{} (db={})",
                master_cfg.host,
                master_cfg.port,
                master_cfg.db);
#endif

            co_return master_cfg;
        }

        RedisError err{RedisErrorCategory::Io, "RedisSentinel: all sentinels failed"};
        co_return std::unexpected(err);
    }
} // namespace usub::uredis