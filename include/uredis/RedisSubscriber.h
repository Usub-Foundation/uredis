#ifndef REDISSUBSCRIBER_H
#define REDISSUBSCRIBER_H

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncEvent.h"
#include "uvent/sync/AsyncMutex.h"
#include "uvent/utils/buffer/DynamicBuffer.h"

#include "uredis/RedisTypes.h"
#include "uredis/RespParser.h"
#include "uredis/RedisClient.h" // для RedisConfig, RedisResult

namespace usub::uredis
{
    namespace net = usub::uvent::net;
    namespace sync = usub::uvent::sync;
    namespace task = usub::uvent::task;
    namespace system = usub::uvent::system;

    class RedisSubscriber
    {
    public:
        using MessageCallback = std::function<void(const std::string& channel,
                                                   const std::string& payload)>;

        explicit RedisSubscriber(RedisConfig cfg);

        task::Awaitable<RedisResult<void>> connect();

        task::Awaitable<RedisResult<void>> subscribe(std::string channel, MessageCallback cb);
        task::Awaitable<RedisResult<void>> psubscribe(std::string pattern, MessageCallback cb);

        task::Awaitable<RedisResult<void>> unsubscribe(std::string channel);
        task::Awaitable<RedisResult<void>> punsubscribe(std::string pattern);

        task::Awaitable<void> close();

        inline bool is_connected() const
        {
            return this->connected_ && !this->closing_;
        }

    private:
        struct PendingSub
        {
            sync::AsyncEvent event{sync::Reset::Manual, false};
            RedisResult<void> result{
                std::unexpected(RedisError{RedisErrorCategory::Protocol, "uninitialized"})
            };
            MessageCallback callback;
        };

        struct PendingUnsub
        {
            sync::AsyncEvent event{sync::Reset::Manual, false};
            RedisResult<void> result{
                std::unexpected(RedisError{RedisErrorCategory::Protocol, "uninitialized"})
            };
        };

        RedisConfig config_;

        net::TCPClientSocket socket_{};
        std::shared_ptr<detail::TlsSession> tls_{}; // set only when config_.tls.enabled
        bool connected_{false};
        bool closing_{false};

        RespParser parser_{};

        // Plain socket or TLS session, whichever the connection uses.
        task::Awaitable<ssize_t> write_frame(std::vector<std::uint8_t>& frame);

        sync::AsyncMutex write_mutex_;

        std::unordered_map<std::string, std::shared_ptr<PendingSub>> pending_sub_;
        std::unordered_map<std::string, std::shared_ptr<PendingSub>> pending_psub_;
        std::unordered_map<std::string, std::shared_ptr<PendingUnsub>> pending_unsub_;
        std::unordered_map<std::string, std::shared_ptr<PendingUnsub>> pending_punsub_;

        std::unordered_map<std::string, MessageCallback> channel_handlers_;
        std::unordered_map<std::string, MessageCallback> pattern_handlers_;

        task::Awaitable<void> reader_loop();

        static std::vector<uint8_t> encode_command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        void handle_array(RedisValue&& v);
        void fail_all(RedisErrorCategory cat, std::string_view msg);
    };
} // namespace usub::uredis

#endif //REDISSUBSCRIBER_H
