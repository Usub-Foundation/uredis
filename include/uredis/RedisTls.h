#ifndef UREDIS_REDISTLS_H
#define UREDIS_REDISTLS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncMutex.h"

#include "uredis/RedisTypes.h"

// Opaque OpenSSL handles: consumers of uredis never need OpenSSL headers.
struct ssl_st;
struct ssl_ctx_st;
struct bio_st;

namespace usub::uredis {
    namespace net = usub::uvent::net;
    namespace task = usub::uvent::task;

    // TLS for the connection to Redis. Off by default: with enabled == false nothing below is
    // touched and the client behaves exactly as before.
    struct RedisTlsConfig {
        bool enabled{false};

        // Verify the server certificate chain and its name. Turning this off encrypts the traffic
        // but accepts any server (no protection against an active man-in-the-middle).
        bool verify_peer{true};

        // Name to verify the certificate against and to send as SNI. Defaults to the host the
        // client connects to. Set it explicitly for clusters: nodes are reached by the IPs that
        // CLUSTER SLOTS reports, while the certificate usually carries the service DNS name.
        // An IP address here is matched against the certificate's IP SANs (no SNI is sent).
        std::optional<std::string> server_name;

        // PEM bundle of trusted CAs. Defaults to the system trust store.
        std::optional<std::string> ca_file;

        // Client certificate (PEM chain) and private key for servers with tls-auth-clients yes.
        std::optional<std::string> cert_file;
        std::optional<std::string> key_file;
    };

    // false when uredis was built without OpenSSL (UREDIS_TLS=OFF or OpenSSL not found); a
    // connection with tls.enabled then fails with RedisErrorCategory::Io.
    bool tls_supported() noexcept;

    namespace detail {
        // One TLS session over an already connected uvent socket. OpenSSL works on memory BIOs;
        // all network I/O stays on the uvent socket, so timeouts and cancellation behave as for
        // plain connections. A std::mutex guards OpenSSL calls (never held across co_await) and
        // an AsyncMutex orders the ciphertext written to the socket, so one reader and one writer
        // may use the session concurrently (RedisSubscriber does).
        class TlsSession {
        public:
            // host: what the client connected to (default verification name).
            static RedisResult<std::shared_ptr<TlsSession> > create(const RedisTlsConfig &cfg, std::string_view host);

            ~TlsSession();

            TlsSession(const TlsSession &) = delete;

            TlsSession &operator=(const TlsSession &) = delete;

            task::Awaitable<RedisResult<void> > handshake(net::TCPClientSocket &socket, int io_timeout_ms);

            // Encrypts and sends all n bytes. Returns n, or <= 0 if the socket failed / timed out.
            task::Awaitable<ssize_t> write_all(net::TCPClientSocket &socket, const std::uint8_t *data, std::size_t n,
                                               int io_timeout_ms);

            // Up to max plaintext bytes into dst. > 0 bytes read, 0 peer closed, < 0 error / timeout.
            task::Awaitable<ssize_t> read_some(net::TCPClientSocket &socket, std::uint8_t *dst, std::size_t max,
                                               int io_timeout_ms);

            // Last OpenSSL failure, for error messages.
            std::string last_error() const;

        private:
            TlsSession() = default;

            // Moves pending ciphertext out of the write BIO. Caller holds mu_.
            std::string drain_out_locked();

            task::Awaitable<bool> send_raw(net::TCPClientSocket &socket, std::string bytes, int io_timeout_ms);

            task::Awaitable<bool> flush(net::TCPClientSocket &socket, int io_timeout_ms);

            task::Awaitable<ssize_t> recv_raw(net::TCPClientSocket &socket, int io_timeout_ms);

            mutable std::mutex mu_;
            usub::uvent::sync::AsyncMutex write_order_;
            std::shared_ptr<ssl_ctx_st> ctx_;
            ssl_st *ssl_{nullptr};
            bio_st *in_{nullptr};  // network → OpenSSL (owned by ssl_)
            bio_st *out_{nullptr}; // OpenSSL → network (owned by ssl_)
            std::string error_;
        };
    } // namespace detail
} // namespace usub::uredis

#endif // UREDIS_REDISTLS_H
