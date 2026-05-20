# RedisClient

`RedisClient` represents a single async connection to Redis.

```cpp
namespace usub::uredis {

struct RedisConfig {
    std::string host;
    std::uint16_t port{6379};
    int db{0};

    std::optional<std::string> username;
    std::optional<std::string> password;

    int connect_timeout_ms{5000};
    int io_timeout_ms{5000};

    // Optional total budget for a single command (0 = disabled, default).
    // When set, the call returns RedisErrorCategory::Timeout if the budget
    // expires before the reply arrives, and the connection is hard-closed
    // so the caller can always exit.
    int command_timeout_ms{0};
};

class RedisClient {
public:
    explicit RedisClient(RedisConfig cfg);

    task::Awaitable<RedisResult<void>> connect();

    task::Awaitable<RedisResult<RedisValue>> command(
        std::string_view cmd,
        std::span<const std::string_view> args);

    template <typename... Args>
    task::Awaitable<RedisResult<RedisValue>> command(
        std::string_view cmd,
        Args&&... args);

    // Per-call timeout overrides RedisConfig::command_timeout_ms.
    // Pass 0 to disable for this specific call.
    task::Awaitable<RedisResult<RedisValue>> command_timed(
        std::string_view cmd,
        std::span<const std::string_view> args,
        int timeout_ms);

    template <typename... Args>
    task::Awaitable<RedisResult<RedisValue>> command_timed(
        int timeout_ms,
        std::string_view cmd,
        Args&&... args);

    task::Awaitable<RedisResult<std::optional<std::string>>> get(std::string_view key);
    task::Awaitable<RedisResult<void>> set(std::string_view key, std::string_view value);
    task::Awaitable<RedisResult<void>> setex(std::string_view key, int ttl_sec, std::string_view value);
    task::Awaitable<RedisResult<int64_t>> del(std::span<const std::string_view> keys);
    task::Awaitable<RedisResult<int64_t>> incrby(std::string_view key, int64_t delta);

    task::Awaitable<RedisResult<int64_t>> hset(
        std::string_view key,
        std::string_view field,
        std::string_view value);

    task::Awaitable<RedisResult<std::optional<std::string>>> hget(
        std::string_view key,
        std::string_view field);

    task::Awaitable<RedisResult<std::unordered_map<std::string, std::string>>> hgetall(
        std::string_view key);

    task::Awaitable<RedisResult<int64_t>> sadd(
        std::string_view key,
        std::span<const std::string_view> members);

    task::Awaitable<RedisResult<int64_t>> srem(
        std::string_view key,
        std::span<const std::string_view> members);

    task::Awaitable<RedisResult<std::vector<std::string>>> smembers(
        std::string_view key);

    task::Awaitable<RedisResult<int64_t>> lpush(
        std::string_view key,
        std::span<const std::string_view> values);

    task::Awaitable<RedisResult<std::vector<std::string>>> lrange(
        std::string_view key,
        int64_t start,
        int64_t stop);

    task::Awaitable<RedisResult<int64_t>> zadd(
        std::string_view key,
        std::span<const std::pair<std::string, double>> members);

    task::Awaitable<RedisResult<std::vector<std::pair<std::string, double>>>> zrange_with_scores(
        std::string_view key,
        int64_t start,
        int64_t stop);

    const RedisConfig& config() const;
};

} // namespace usub::uredis
```

## Connection

```cpp
RedisConfig cfg;
cfg.host = "127.0.0.1";
cfg.port = 15100;
cfg.db   = 0;
// cfg.username = "...";
// cfg.password = "...";

RedisClient client{cfg};
auto rc = co_await client.connect();
if (!rc) {
    const auto& err = rc.error();
    // handle error.category / error.message
}
```

If `username`/`password` are set, the client sends `AUTH`. If `db != 0`, it sends `SELECT db`.

## Raw commands

Low-level wrapper around RESP:

```cpp
auto resp = co_await client.command("PING");
if (!resp) {
    // RedisResult error: network / protocol / server reply
} else {
    const RedisValue& v = *resp;
}
```

The variadic helper builds `std::string_view` array for you:

```cpp
auto resp = co_await client.command("SET", "foo", "bar");
```

## Typed helpers

### Strings

```cpp
co_await client.set("foo", "bar");

auto g = co_await client.get("foo");
if (g && g.value().has_value()) {
    std::string v = g.value().value();
}

co_await client.setex("temp", 60, "value");
```

### Keys

```cpp
std::string_view keys_arr[] = {"k1", "k2", "k3"};
auto d = co_await client.del(std::span{keys_arr, 3});
if (d) {
    int64_t removed = d.value();
}
```

### Counters

```cpp
auto r = co_await client.incrby("counter", 1);
if (r) {
    int64_t value = r.value();
}
```

### Hashes

```cpp
co_await client.hset("user:1", "name", "Kirill");
auto name = co_await client.hget("user:1", "name");
auto all  = co_await client.hgetall("user:1");
```

### Sets

```cpp
std::string_view tags[] = {"foo", "bar", "baz"};
co_await client.sadd("tags", std::span{tags, 3});
auto members = co_await client.smembers("tags");
```

### Lists

```cpp
std::string_view jobs[] = {"job1", "job2", "job3"};
co_await client.lpush("queue", std::span{jobs, 3});
auto list = co_await client.lrange("queue", 0, -1);
```

### Sorted sets

```cpp
std::pair<std::string, double> members[] = {
    {"user1", 10.0},
    {"user2", 20.0},
};
co_await client.zadd("scores", std::span{members, 2});
auto ranking = co_await client.zrange_with_scores("scores", 0, -1);
```

## Error handling

Most API returns `RedisResult<T> = std::expected<T, RedisError>`:

```cpp
if (!resp) {
    const RedisError& err = resp.error();
    // err.category: Io / Protocol / ServerReply / Timeout
    // err.message: string
}
```

## Timeouts

There are two layers of timeouts:

- `io_timeout_ms` — per-IO socket timeout. Bounds the time a single `read`
  or `write` syscall may block. Always on.
- `command_timeout_ms` — optional total budget for a single logical command
  (write + parse + read). `0` (default) disables this layer.

When a command exceeds `command_timeout_ms`, the call returns
`RedisErrorCategory::Timeout`, the underlying socket is hard-closed, and the
next call on the same `RedisClient` will reconnect. This guarantees the
caller can always exit within the configured budget, even when the server
stops sending data mid-reply.

Two ways to set it:

```cpp
// 1) Config default for every command.
RedisConfig cfg{
    .host = "127.0.0.1",
    .port = 6379,
    .io_timeout_ms = 5000,
    .command_timeout_ms = 2000, // 2s budget per command
};
RedisClient client{cfg};

auto r = co_await client.get("key"); // bounded by 2s

// 2) Per-call override (wins over the config value).
auto r2 = co_await client.command_timed(500, "BLPOP", "queue", "0");
if (!r2 && r2.error().category == RedisErrorCategory::Timeout) {
    // caller is back in 500ms regardless of server state
}
```

`command_timed(0, ...)` disables the budget for that one call.