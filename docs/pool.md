# RedisPool

`RedisPool` is a simple round-robin pool of `RedisClient` connections.

```cpp
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

    // Optional total budget per command, forwarded to each pooled
    // RedisClient. 0 = disabled (default). See docs/client.md.
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
        Args&&... args);

    // Per-call timeout override (see RedisClient::command_timed).
    task::Awaitable<RedisResult<RedisValue>> command_timed(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms);

    template <typename... Args>
    task::Awaitable<RedisResult<RedisValue>> command_timed(
        int timeout_ms,
        std::string_view cmd,
        Args&&... args);
};
```

## Creating a pool

```cpp
RedisPoolConfig pcfg;
pcfg.host = "127.0.0.1";
pcfg.port = 15100;
pcfg.db   = 0;
pcfg.size = 8;

RedisPool pool{pcfg};
auto rc = co_await pool.connect_all();
if (!rc) {
    const auto& err = rc.error();
    // handle
}
```

`connect_all()` iterates over all internal `RedisClient` instances and connects them one by one.

## Using the pool

```cpp
auto r = co_await pool.command("INCRBY", "counter", "1");
if (!r) {
    const auto& err = r.error();
    // handle
} else {
    const RedisValue& v = *r;
    if (v.is_integer()) {
        int64_t value = v.as_integer();
    }
}
```

Clients are selected using an atomic round-robin index:

```cpp
std::size_t idx =
    rr_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
```

The pool itself only exposes the low-level `command()` API. You can easily wrap it to get higher-level helpers if
needed.