#ifndef UREDIS_REDISCLUSTERCLIENT_H
#define UREDIS_REDISCLUSTERCLIENT_H

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncMutex.h"
#include "uvent/sync/AsyncEvent.h"
#include "uvent/sync/AsyncSemaphore.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"

#include "uredis/RedisClient.h"
#include "uredis/RedisTypes.h"

namespace usub::uredis {
    namespace task = usub::uvent::task;
    namespace sync = usub::uvent::sync;

    struct RedisClusterNode {
        std::string host;
        std::uint16_t port{6379};
    };

    struct RedisClusterConfig {
        std::vector<RedisClusterNode> seeds;

        std::optional<std::string> username;
        std::optional<std::string> password;

        int connect_timeout_ms{5000};
        int io_timeout_ms{5000};

        // Optional total time budget for a single cluster command (across all
        // attempts on a single node). 0 = disabled. On expiration the call
        // returns RedisErrorCategory::Timeout, the underlying connection is
        // hard-closed (so it leaves the pool) and the slot mapping is
        // invalidated so the next command rediscovers the cluster topology.
        int command_timeout_ms{0};

        int max_redirections{5};
        std::size_t max_connections_per_node{4};

        bool force_standalone{false};
    };

    class RedisClusterClient {
    public:
        explicit RedisClusterClient(RedisClusterConfig cfg);

        task::Awaitable<RedisResult<void>> connect();

        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            Args&&... args) {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()));
        }

        // Per-call timeout variant: total budget across all redirection
        // attempts. On expiration: Timeout error + slot mapping invalidated
        // (next command will trigger a full rediscover).
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            std::string_view cmd,
            std::span<const std::string_view> args,
            int timeout_ms);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command_timed(
            int timeout_ms,
            std::string_view cmd,
            Args&&... args) {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command_timed(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()),
                timeout_ms);
        }

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
        get_client_for_key(std::string_view key);

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
        get_any_client();

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
        get_client_for_slot(int slot);

    private:
        struct Node {
            RedisConfig cfg;

            usub::queue::concurrent::MPMCQueue<std::shared_ptr<RedisClient>> idle;
            std::atomic<std::size_t> live_count{0};

            sync::AsyncSemaphore idle_sem{0};
            std::atomic<std::uint32_t> waiters{0};

            Node(RedisConfig cfg_, std::size_t max_pool)
                : cfg(std::move(cfg_))
                , idle(max_pool) {}

            void notify_waiters_if_any() noexcept {
                if (waiters.load(std::memory_order_relaxed) > 0)
                    idle_sem.release();
            }
        };

        struct PooledClient {
            std::shared_ptr<Node> node;
            std::shared_ptr<RedisClient> client;
        };

        enum class RedirType { None, Moved, Ask };

        struct Redirection {
            RedirType type{RedirType::None};
            int slot{-1};
            std::string host;
            std::uint16_t port{0};
        };

        RedisClusterConfig cfg_;

        std::vector<std::shared_ptr<Node>> nodes_;
        std::array<int, 16384> slot_to_node_{};
        bool standalone_mode_{false};

        sync::AsyncMutex mutex_;

        sync::AsyncMutex init_mutex_;
        sync::AsyncEvent init_event_{sync::Reset::Manual, false};
        bool init_started_{false};
        bool init_finished_{false};
        std::optional<RedisResult<void>> init_result_;

        sync::AsyncMutex rediscover_mutex_;

        static std::string_view extract_hash_tag(std::string_view key);
        static std::uint16_t calc_slot(std::string_view key);
        static std::optional<Redirection> parse_redirection(const std::string& msg);
        static bool is_slot_mapping_empty_error(const RedisError& e) noexcept;

        RedisResult<int> node_index_for_slot_locked(int slot) const;
        RedisResult<int> node_index_for_key_locked(std::string_view key) const;

        int ensure_node_locked(std::string_view host, std::uint16_t port);

        void setup_standalone_locked();
        bool has_full_slot_mapping_locked() const noexcept;

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
        connect_to_node(std::string_view host, std::uint16_t port);

        static task::Awaitable<void> warm_pool_fill_to_max(
            const RedisClusterConfig& cfg,
            const std::shared_ptr<Node>& node);

        task::Awaitable<RedisResult<PooledClient>>
        acquire_from_node(const std::shared_ptr<Node>& node);

        task::Awaitable<void>
        release_pooled(PooledClient&& pc, bool faulty);

        task::Awaitable<RedisResult<PooledClient>> acquire_for_slot(int slot);
        task::Awaitable<RedisResult<PooledClient>> acquire_for_any();
        task::Awaitable<RedisResult<PooledClient>> acquire_for_key(std::string_view key);

        task::Awaitable<RedisResult<void>> initial_discovery();
        task::Awaitable<RedisResult<void>> rediscover_slots_serialized();

        // Mark every slot as unassigned. The next command will hit the
        // "slot mapping is empty" path and trigger rediscover automatically.
        // No-op in standalone mode.
        void invalidate_slot_mapping_locked() noexcept;

        // Coroutine wrapper that takes the mutex and invalidates the mapping.
        // Used after a Timeout so the next command rebuilds the topology.
        task::Awaitable<void> force_rebuild_on_timeout();

        task::Awaitable<RedisResult<RedisValue>> command_impl(
            std::string_view cmd,
            std::span<const std::string_view> args,
            int timeout_ms);

        task::Awaitable<void> apply_moved(const Redirection& r);

        task::Awaitable<RedisResult<RedisValue>> execute_ask(
            const Redirection& r,
            std::string_view cmd,
            std::span<const std::string_view> args,
            int per_call_timeout_ms);
    };
} // namespace usub::uredis

#endif // UREDIS_REDISCLUSTERCLIENT_H
