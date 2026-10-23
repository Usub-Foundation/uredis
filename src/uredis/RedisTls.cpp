#include "uredis/RedisTls.h"

#include <sys/types.h>

#ifdef UREDIS_HAS_TLS

#include <arpa/inet.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <map>
#include <tuple>
#include <vector>

#endif

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis {
#ifndef UREDIS_HAS_TLS

    bool tls_supported() noexcept { return false; }

    namespace detail {
        RedisResult<std::shared_ptr<TlsSession> > TlsSession::create(const RedisTlsConfig &, std::string_view) {
            return std::unexpected(RedisError{RedisErrorCategory::Io, "uredis was built without TLS support"});
        }

        TlsSession::~TlsSession() = default;

        task::Awaitable<RedisResult<void> > TlsSession::handshake(net::TCPClientSocket &, int) {
            co_return std::unexpected(RedisError{RedisErrorCategory::Io, "uredis was built without TLS support"});
        }

        task::Awaitable<ssize_t> TlsSession::write_all(net::TCPClientSocket &, const std::uint8_t *, std::size_t, int) {
            co_return -1;
        }

        task::Awaitable<ssize_t> TlsSession::read_some(net::TCPClientSocket &, std::uint8_t *, std::size_t, int) {
            co_return -1;
        }

        std::string TlsSession::last_error() const { return "uredis was built without TLS support"; }

        std::string TlsSession::drain_out_locked() { return {}; }

        task::Awaitable<bool> TlsSession::send_raw(net::TCPClientSocket &, std::string, int) { co_return false; }

        task::Awaitable<bool> TlsSession::flush(net::TCPClientSocket &, int) { co_return false; }

        task::Awaitable<ssize_t> TlsSession::recv_raw(net::TCPClientSocket &, int) { co_return -1; }
    } // namespace detail

#else

    bool tls_supported() noexcept { return true; }

    namespace detail {
        namespace {
            constexpr std::size_t kNetChunk = 16 * 1024 + 512; // one TLS record + overhead

            // All queued OpenSSL errors as one line; empties the (thread-local) error queue.
            std::string openssl_errors() {
                std::string out;
                unsigned long e = 0;
                char buf[256];
                while ((e = ::ERR_get_error()) != 0) {
                    ::ERR_error_string_n(e, buf, sizeof(buf));
                    if (!out.empty()) out += "; ";
                    out += buf;
                }
                return out;
            }

            bool is_ip_literal(const std::string &name) {
                unsigned char tmp[16];
                return ::inet_pton(AF_INET, name.c_str(), tmp) == 1 || ::inet_pton(AF_INET6, name.c_str(), tmp) == 1;
            }

            // SSL_CTX is costly to build (trust store, key parsing) and immutable once built, so
            // connections with the same settings share one.
            using CtxKey = std::tuple<bool, std::string, std::string, std::string>;

            RedisResult<std::shared_ptr<ssl_ctx_st> > shared_ctx(const RedisTlsConfig &cfg) {
                static std::mutex mu;
                static std::map<CtxKey, std::weak_ptr<ssl_ctx_st> > cache;

                CtxKey key{cfg.verify_peer, cfg.ca_file.value_or(""), cfg.cert_file.value_or(""),
                           cfg.key_file.value_or("")};
                std::lock_guard lock(mu);
                if (auto it = cache.find(key); it != cache.end())
                    if (auto ctx = it->second.lock()) return ctx;

                SSL_CTX *raw = ::SSL_CTX_new(::TLS_client_method());
                if (!raw)
                    return std::unexpected(RedisError{RedisErrorCategory::Io, "SSL_CTX_new: " + openssl_errors()});
                std::shared_ptr<ssl_ctx_st> ctx(raw, [](SSL_CTX *c) { ::SSL_CTX_free(c); });

                SSL_CTX_set_min_proto_version(raw, TLS1_2_VERSION);
                SSL_CTX_set_mode(raw, SSL_MODE_ENABLE_PARTIAL_WRITE);

                if (cfg.verify_peer) {
                    ::SSL_CTX_set_verify(raw, SSL_VERIFY_PEER, nullptr);
                    const int ok = cfg.ca_file
                                       ? ::SSL_CTX_load_verify_locations(raw, cfg.ca_file->c_str(), nullptr)
                                       : ::SSL_CTX_set_default_verify_paths(raw);
                    if (ok != 1)
                        return std::unexpected(RedisError{
                            RedisErrorCategory::Io, "TLS trust store (" + cfg.ca_file.value_or("system") + "): " +
                                                    openssl_errors()
                        });
                } else {
                    ::SSL_CTX_set_verify(raw, SSL_VERIFY_NONE, nullptr);
                }

                if (cfg.cert_file || cfg.key_file) {
                    if (!cfg.cert_file || !cfg.key_file)
                        return std::unexpected(RedisError{
                            RedisErrorCategory::Io, "TLS client certificate needs both cert_file and key_file"
                        });
                    if (::SSL_CTX_use_certificate_chain_file(raw, cfg.cert_file->c_str()) != 1 ||
                        ::SSL_CTX_use_PrivateKey_file(raw, cfg.key_file->c_str(), SSL_FILETYPE_PEM) != 1 ||
                        ::SSL_CTX_check_private_key(raw) != 1)
                        return std::unexpected(RedisError{
                            RedisErrorCategory::Io, "TLS client certificate: " + openssl_errors()
                        });
                }

                cache[key] = ctx;
                return ctx;
            }
        } // namespace

        RedisResult<std::shared_ptr<TlsSession> > TlsSession::create(const RedisTlsConfig &cfg, std::string_view host) {
            auto ctx = shared_ctx(cfg);
            if (!ctx) return std::unexpected(ctx.error());

            std::shared_ptr<TlsSession> s(new TlsSession());
            s->ctx_ = std::move(*ctx);
            s->ssl_ = ::SSL_new(s->ctx_.get());
            BIO *in = ::BIO_new(::BIO_s_mem());
            BIO *out = ::BIO_new(::BIO_s_mem());
            if (!s->ssl_ || !in || !out) {
                if (in) ::BIO_free(in);
                if (out) ::BIO_free(out);
                return std::unexpected(RedisError{RedisErrorCategory::Io, "SSL_new: " + openssl_errors()});
            }
            // An empty memory BIO means "no data yet", not EOF.
            BIO_set_mem_eof_return(in, -1);
            BIO_set_mem_eof_return(out, -1);
            ::SSL_set_bio(s->ssl_, in, out); // ssl_ owns both from here
            s->in_ = in;
            s->out_ = out;
            ::SSL_set_connect_state(s->ssl_);

            const std::string name = cfg.server_name.value_or(std::string{host});
            if (!name.empty()) {
                const bool ip = is_ip_literal(name);
                if (!ip) SSL_set_tlsext_host_name(s->ssl_, name.c_str()); // SNI is for DNS names only
                if (cfg.verify_peer) {
                    X509_VERIFY_PARAM *param = ::SSL_get0_param(s->ssl_);
                    const int ok = ip
                                       ? ::X509_VERIFY_PARAM_set1_ip_asc(param, name.c_str())
                                       : ::X509_VERIFY_PARAM_set1_host(param, name.c_str(), name.size());
                    if (ok != 1)
                        return std::unexpected(RedisError{
                            RedisErrorCategory::Io, "TLS verification name '" + name + "': " + openssl_errors()
                        });
                }
            }
            return s;
        }

        TlsSession::~TlsSession() {
            if (ssl_) ::SSL_free(ssl_); // frees in_ / out_ too
        }

        std::string TlsSession::last_error() const {
            std::lock_guard lock(mu_);
            return error_;
        }

        std::string TlsSession::drain_out_locked() {
            std::string bytes;
            char chunk[kNetChunk];
            while (BIO_pending(out_) > 0) {
                const int n = ::BIO_read(out_, chunk, static_cast<int>(sizeof(chunk)));
                if (n <= 0) break;
                bytes.append(chunk, static_cast<std::size_t>(n));
            }
            return bytes;
        }

        task::Awaitable<bool> TlsSession::send_raw(net::TCPClientSocket &socket, std::string bytes, int io_timeout_ms) {
            std::size_t off = 0;
            while (off < bytes.size()) {
                socket.set_timeout_ms(static_cast<std::uint64_t>(io_timeout_ms));
                const ssize_t n = co_await socket.async_write(reinterpret_cast<std::uint8_t *>(bytes.data() + off),
                                                              bytes.size() - off);
                if (n <= 0) co_return false;
                off += static_cast<std::size_t>(n);
            }
            co_return true;
        }

        task::Awaitable<bool> TlsSession::flush(net::TCPClientSocket &socket, int io_timeout_ms) {
            // Drain and send under one lock so records reach the socket in the order OpenSSL made them.
            auto guard = co_await write_order_.lock();
            if (!guard.owns_lock()) co_return false;
            std::string bytes;
            {
                std::lock_guard lock(mu_);
                bytes = drain_out_locked();
            }
            if (bytes.empty()) co_return true;
            co_return co_await send_raw(socket, std::move(bytes), io_timeout_ms);
        }

        task::Awaitable<ssize_t> TlsSession::recv_raw(net::TCPClientSocket &socket, int io_timeout_ms) {
            std::vector<std::uint8_t> buf(kNetChunk);
            socket.set_timeout_ms(static_cast<std::uint64_t>(io_timeout_ms));
            const ssize_t rd = co_await socket.async_read(buf.data(), buf.size());
            if (rd <= 0) co_return rd;
            std::lock_guard lock(mu_);
            const int w = ::BIO_write(in_, buf.data(), static_cast<int>(rd));
            co_return w == rd ? rd : -1;
        }

        task::Awaitable<RedisResult<void> > TlsSession::handshake(net::TCPClientSocket &socket, int io_timeout_ms) {
            for (;;) {
                int rc = 0;
                int err = 0;
                {
                    std::lock_guard lock(mu_);
                    ::ERR_clear_error();
                    rc = ::SSL_do_handshake(ssl_);
                    err = rc == 1 ? SSL_ERROR_NONE : ::SSL_get_error(ssl_, rc);
                    if (err != SSL_ERROR_NONE && err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
                        error_ = openssl_errors();
                        const long vr = ::SSL_get_verify_result(ssl_);
                        if (vr != X509_V_OK) {
                            if (!error_.empty()) error_ += "; ";
                            error_ += std::string{"certificate: "} + ::X509_verify_cert_error_string(vr);
                        }
                    }
                }
                // Whatever the outcome, send what OpenSSL produced (ClientHello, Finished, alerts).
                if (!co_await flush(socket, io_timeout_ms))
                    co_return std::unexpected(RedisError{RedisErrorCategory::Io, "TLS handshake: write failed"});

                if (err == SSL_ERROR_NONE) {
#ifdef UREDIS_LOGS
                    ulog::info("RedisTls: handshake OK version={} cipher={}", ::SSL_get_version(ssl_),
                               SSL_get_cipher_name(ssl_));
#endif
                    co_return RedisResult<void>{};
                }
                if (err == SSL_ERROR_WANT_WRITE) continue;
                if (err == SSL_ERROR_WANT_READ) {
                    const ssize_t rd = co_await recv_raw(socket, io_timeout_ms);
                    if (rd <= 0)
                        co_return std::unexpected(RedisError{
                            RedisErrorCategory::Io,
                            rd == 0 ? "TLS handshake: connection closed by server" : "TLS handshake: read failed"
                        });
                    continue;
                }
                co_return std::unexpected(RedisError{RedisErrorCategory::Io, "TLS handshake failed: " + last_error()});
            }
        }

        task::Awaitable<ssize_t> TlsSession::write_all(net::TCPClientSocket &socket, const std::uint8_t *data,
                                                       std::size_t n, int io_timeout_ms) {
            auto guard = co_await write_order_.lock();
            if (!guard.owns_lock()) co_return -1;
            std::size_t off = 0;
            while (off < n) {
                int rc = 0;
                int err = 0;
                std::string bytes;
                {
                    std::lock_guard lock(mu_);
                    ::ERR_clear_error();
                    rc = ::SSL_write(ssl_, data + off, static_cast<int>(std::min<std::size_t>(n - off, 1 << 20)));
                    err = rc > 0 ? SSL_ERROR_NONE : ::SSL_get_error(ssl_, rc);
                    if (err != SSL_ERROR_NONE) error_ = openssl_errors();
                    bytes = drain_out_locked();
                }
                if (!bytes.empty() && !co_await send_raw(socket, std::move(bytes), io_timeout_ms)) co_return -1;
                if (err != SSL_ERROR_NONE) co_return -1; // memory BIO never blocks writes
                off += static_cast<std::size_t>(rc);
            }
            co_return static_cast<ssize_t>(n);
        }

        task::Awaitable<ssize_t> TlsSession::read_some(net::TCPClientSocket &socket, std::uint8_t *dst, std::size_t max,
                                                       int io_timeout_ms) {
            for (;;) {
                int rc = 0;
                int err = 0;
                bool has_output = false;
                {
                    std::lock_guard lock(mu_);
                    ::ERR_clear_error();
                    rc = ::SSL_read(ssl_, dst, static_cast<int>(std::min<std::size_t>(max, 1 << 20)));
                    err = rc > 0 ? SSL_ERROR_NONE : ::SSL_get_error(ssl_, rc);
                    if (err != SSL_ERROR_NONE && err != SSL_ERROR_WANT_READ && err != SSL_ERROR_ZERO_RETURN)
                        error_ = openssl_errors();
                    has_output = BIO_pending(out_) > 0;
                }
                // Reading may produce records to send (TLS 1.3 key update replies, alerts).
                if (has_output && !co_await flush(socket, io_timeout_ms)) co_return -1;

                if (err == SSL_ERROR_NONE) co_return static_cast<ssize_t>(rc);
                if (err == SSL_ERROR_ZERO_RETURN) co_return 0; // close_notify
                if (err != SSL_ERROR_WANT_READ) co_return -1;

                const ssize_t rd = co_await recv_raw(socket, io_timeout_ms);
                if (rd <= 0) co_return rd;
            }
        }
    } // namespace detail

#endif
} // namespace usub::uredis
