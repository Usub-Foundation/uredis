# TLS

TLS is optional and **off by default**: a config without `tls.enabled = true` connects over plain
TCP exactly as before, and nothing TLS-related runs.

```cpp
namespace usub::uredis {

struct RedisTlsConfig {
    bool enabled{false};
    bool verify_peer{true};                    // chain + name check
    std::optional<std::string> server_name;    // name to verify / SNI; default = host
    std::optional<std::string> ca_file;        // PEM bundle; default = system trust store
    std::optional<std::string> cert_file;      // client certificate (mTLS), PEM chain
    std::optional<std::string> key_file;       // its private key
};

bool tls_supported() noexcept;                 // false if built without OpenSSL
}
```

The field is `tls` in `RedisConfig`, `RedisPoolConfig` and `RedisClusterConfig`; `RedisSubscriber`
and `RedisBus` use it through their `RedisConfig`. Sentinel: `RedisSentinelConfig::sentinel_tls` for
the sentinels themselves, `base_redis.tls` for the resolved master.

```cpp
RedisConfig cfg;
cfg.host = "redis.auth.svc.cluster.local";
cfg.port = 6380;
cfg.password = "...";
cfg.tls.enabled = true;
cfg.tls.ca_file = "/etc/redis-tls/ca.crt";   // CA that signed the server certificate

RedisClient client{cfg};
co_await client.connect();                   // TCP connect → TLS handshake → AUTH/SELECT
```

## Behaviour

- TLS 1.2 minimum. The handshake runs right after the TCP connect, bounded by `io_timeout_ms`
  per network operation; a failed handshake is `RedisErrorCategory::Io` with the OpenSSL reason
  (including the certificate verification error).
- A bad `ca_file` / client certificate is reported on `connect()` without a retry.
- OpenSSL works on memory BIOs; all network I/O stays on the uvent socket, so timeouts,
  `command_timeout_ms` and cancellation behave as on plain connections.
- `verify_peer = false` encrypts but accepts any server — no protection against an active
  man-in-the-middle. Use it only for debugging.
- An IP address in `server_name` (or as the host when `server_name` is unset) is matched against
  the certificate's IP SANs and no SNI is sent.
- **Cluster:** nodes are reached by the addresses `CLUSTER SLOTS` reports (often pod IPs). Either set
  `tls.server_name` to the name the certificate carries, or put the node IPs into its SANs.
- Connections with identical TLS settings share one `SSL_CTX` (trust store loaded once).

## Building

CMake option `UREDIS_TLS` (default `ON`) links OpenSSL when it is found. Without OpenSSL the library
still builds; `tls_supported()` returns `false` and a TLS connection fails with
`RedisErrorCategory::Io`.

## Server side (Redis ≥ 6)

```
port 0                      # or keep the plain port for migration
tls-port 6380
tls-cert-file /tls/tls.crt
tls-key-file  /tls/tls.key
tls-ca-cert-file /tls/ca.crt
tls-auth-clients no         # "yes" requires cert_file/key_file on the client (mTLS)
```

## Test

`tests/tls/run.sh <path-to-uredis_tls_test>` generates a CA and certificates, starts Redis with a
plain and a TLS port in Docker and runs `tests/tls/tls_test.cpp`: plain client, TLS client
(1 MiB values, 300 sequential commands), wrong name / unknown CA / plain-text server rejected,
`verify_peer=false`, mTLS, cluster client, pool, subscriber. Build it with `-DBUILD_TESTING=ON`.
