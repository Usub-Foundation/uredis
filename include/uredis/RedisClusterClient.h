#ifndef UREDIS_REDISCLUSTERCLIENT_H
#define UREDIS_REDISCLUSTERCLIENT_H

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
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

        // Optional total time budget for a single cluster command (discovery
        // wait, pool acquisition, IO and MOVED/ASK hops). 0 = disabled. On
        // expiration the call returns RedisErrorCategory::Timeout. Only a
        // connection whose in-flight command timed out is hard-closed; a
        // connection the call never used goes back to the pool. A timeout does
        // not touch the slot mapping: topology is rebuilt on MOVED and on
        // connection (Io) errors, single-flighted across concurrent callers.
        int command_timeout_ms{0};

        int max_redirections{5};
        std::size_t max_connections_per_node{4};

        bool force_standalone{false};

        // TLS for every node connection; disabled by default. Nodes are reached by the addresses
        // CLUSTER SLOTS reports (usually pod IPs) — set tls.server_name to the name the
        // certificate carries, or include those IPs in its SANs.
        RedisTlsConfig tls{};
    };

    class RedisClusterClient {
    public:
        explicit RedisClusterClient(RedisClusterConfig cfg);

        // Runs the initial CLUSTER SLOTS discovery. Only a success is cached:
        // after a failure (seeds down, DNS not ready) the next connect() — or
        // the next command, which calls it implicitly — tries again. Concurrent
        // callers share one discovery run and all get its result.
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
        // attempts. On expiration: Timeout error (see command_timeout_ms).
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

            // idle_sem is only a wake-up hint for coroutines parked in
            // acquire_from_node, so it is released only while someone waits —
            // releasing on every return to the pool (fast-path acquires never
            // take a permit) piled up permits without bound: parked acquirers
            // then spun through them without suspending, and the int32 counter
            // eventually overflowed. seq_cst here pairs with the waiter's
            // waiters++ / re-check: either the waiter sees the freed
            // connection (slot) or we see the waiter.
            void notify_waiters_if_any() noexcept {
                if (waiters.load(std::memory_order_seq_cst) > 0)
                    idle_sem.release();
            }

            // A healthy connection goes back to the idle queue.
            bool push_idle(std::shared_ptr<RedisClient> c) noexcept {
                if (!idle.try_enqueue(std::move(c)))
                    return false;
                std::atomic_thread_fence(std::memory_order_seq_cst);
                notify_waiters_if_any();
                return true;
            }

            // A pooled connection left the pool for good (closed or never
            // opened): free its slot and wake a waiter so it can open a new one.
            void drop_live() noexcept {
                live_count.fetch_sub(1, std::memory_order_seq_cst);
                notify_waiters_if_any();
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

        // One discovery run (initial connect or topology rebuild), shared by
        // every caller that arrives while it is in flight.
        struct DiscoveryFlight {
            sync::AsyncEvent done{sync::Reset::Manual, false};
            RedisResult<void> result{};
            // The leader was cancelled mid-run: its error says nothing about the
            // cluster, so joiners start (or join) a fresh run instead.
            bool aborted{false};
        };

        RedisClusterConfig cfg_;

        std::vector<std::shared_ptr<Node>> nodes_;
        std::array<int, 16384> slot_to_node_{};
        bool standalone_mode_{false};

        sync::AsyncMutex mutex_;

        // Guards discovery_flight_ and writes of init_ok_. Never held across a
        // co_await (plain critical sections of a few loads/stores).
        std::mutex discovery_mu_;
        std::shared_ptr<DiscoveryFlight> discovery_flight_;
        // Set once any discovery succeeded; failures are never cached.
        std::atomic<bool> init_ok_{false};

        // Bumped on every successfully applied topology (full slot map or
        // standalone fallback). A caller records it before routing a command;
        // when the command fails and asks for a rebuild, a moved generation
        // means somebody already rebuilt — it just retries on the new map.
        std::atomic<std::uint64_t> topology_gen_{0};

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
            std::shared_ptr<Node> node);

        // deadline_ms: monotonic ms, 0 = none. Bounds the wait for a free
        // pooled connection (Timeout error when it passes).
        task::Awaitable<RedisResult<PooledClient>>
        acquire_from_node(std::shared_ptr<Node> node, std::int64_t deadline_ms);

        // faulty = the connection had an IO failure or a timed-out in-flight
        // command; anything else goes back to the idle queue.
        static void release_pooled(PooledClient&& pc, bool faulty) noexcept;

        // Callers must have connected (init_ok_) before routing.
        task::Awaitable<RedisResult<PooledClient>> acquire_for_slot(int slot, std::int64_t deadline_ms);
        task::Awaitable<RedisResult<PooledClient>> acquire_for_any(std::int64_t deadline_ms);
        task::Awaitable<RedisResult<PooledClient>> acquire_for_key(std::string_view key, std::int64_t deadline_ms);

        task::Awaitable<RedisResult<void>> initial_discovery();

        // Single-flight discovery. initial: done once init_ok_ is set;
        // otherwise done once topology_gen_ != observed_gen. Callers arriving
        // while a run is in flight wait for it (bounded by deadline_ms when
        // > 0) and take its result instead of starting their own.
        task::Awaitable<RedisResult<void>> run_discovery(
            bool initial, std::uint64_t observed_gen, std::int64_t deadline_ms);

        // observed_gen: topology generation the caller read before routing the
        // command that failed. If it has moved since, the topology was rebuilt
        // concurrently and the CLUSTER SLOTS round-trip is skipped.
        task::Awaitable<RedisResult<void>> rediscover_slots_serialized(
            std::uint64_t observed_gen, std::int64_t deadline_ms);

        task::Awaitable<RedisResult<RedisValue>> command_impl(
            std::string_view cmd,
            std::span<const std::string_view> args,
            int timeout_ms);

        task::Awaitable<void> apply_moved(Redirection r);

        task::Awaitable<RedisResult<RedisValue>> execute_ask(
            Redirection r,
            std::string_view cmd,
            std::span<const std::string_view> args,
            std::int64_t deadline_ms);
    };
} // namespace usub::uredis

#endif // UREDIS_REDISCLUSTERCLIENT_H
