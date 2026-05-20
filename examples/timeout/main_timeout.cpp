//
// Created by kirill on 1/13/26.
//
// Demonstrates the optional per-command timeout introduced for RedisClient
// and RedisClusterClient. Two ways to set it:
//   1) RedisClusterConfig::command_timeout_ms — default budget for every
//      command. Pass 0 (default) to disable.
//   2) command_timed(timeout_ms, ...) — per-call budget that overrides (1).
//
// On expiration the caller receives RedisErrorCategory::Timeout and is
// guaranteed to exit promptly. In cluster mode the slot mapping is
// invalidated, so the next command will rediscover the topology before
// going to a node.
//

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include <uvent/Uvent.h>
#include <uvent/system/SystemContext.h>
#include <uredis/RedisClusterClient.h>
#include <ulog/ulog.h>

namespace {
using namespace std::chrono_literals;

usub::uvent::task::Awaitable<void> test_reconnect(usub::uredis::RedisClusterClient& redis_client) {
    std::uint64_t ok = 0;
    std::uint64_t fail = 0;
    std::uint64_t timed_out = 0;

    for (;;) {
        // Per-call 500ms budget — overrides any config-level default and
        // guarantees we never wait longer than that, regardless of how
        // sick the cluster is.
        auto res = co_await redis_client.command_timed(500, "HGET", "fx:rates", "USD");

        if (!res) {
            const auto& e = res.error();
            if (e.category == usub::uredis::RedisErrorCategory::Timeout) {
                ++timed_out;
                usub::ulog::warn("HGET timed out (cluster mapping will be rebuilt on next call): "
                                 "msg=\"{}\" ok={} fail={} timeout={}",
                                 e.message, ok, fail, timed_out);
            } else {
                ++fail;
                usub::ulog::error("HGET failed: ({}) ok={} fail={} timeout={}",
                                  e.message, ok, fail, timed_out);
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(200ms);
            continue;
        }

        const usub::uredis::RedisValue& v = res.value();
        if (v.is_null()) {
            usub::ulog::warn("No rate for USD (key missing?) ok={} fail={} timeout={}",
                             ok, fail, timed_out);
        } else {
            ++ok;
            usub::ulog::info("USD:{} ok={} fail={} timeout={}",
                             v.as_string(), ok, fail, timed_out);
        }

        co_await usub::uvent::system::this_coroutine::sleep_for(200ms);
    }
}
} // namespace

int main() {
    usub::ulog::ULogInit cfg{
        .trace_path = nullptr,
        .debug_path = nullptr,
        .info_path = nullptr,
        .warn_path = nullptr,
        .error_path = nullptr,
        .critical_path = nullptr,
        .fatal_path = nullptr,
        .flush_interval_ns = 5'000'000'000ULL,
        .queue_capacity = 1024,
        .batch_size = 512,
        .enable_color_stdout = true,
        .max_file_size_bytes = 10 * 1024 * 1024,
        .max_files = 3,
        .json_mode = false,
        .track_metrics = true
    };
    usub::ulog::init(cfg);

    std::string keydb_host = "127.0.0.1";
    std::string keydb_port = "6479";
    std::string keydb_pswd = "devpass";

    usub::Uvent uvent(4);

    usub::uredis::RedisClusterConfig redis_config{
        .seeds = {
            usub::uredis::RedisClusterNode{
                .host = keydb_host,
                .port = static_cast<std::uint16_t>(std::stoi(keydb_port))
            }
        },
        .password = keydb_pswd,
        .connect_timeout_ms = 2000,
        .io_timeout_ms = 2000,
        // Default budget for any command issued via command(...) without an
        // explicit timeout. The coroutine above uses command_timed(500, ...)
        // to override this on a per-call basis.
        .command_timeout_ms = 1000,
        .max_connections_per_node = 16
    };

    usub::uredis::RedisClusterClient redis_cluster_client{redis_config};

    usub::uvent::system::co_spawn(test_reconnect(redis_cluster_client));
    uvent.run();
    return 0;
}
