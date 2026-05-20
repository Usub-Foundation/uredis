# Sentinel Support

uRedis includes full async support for **Redis Sentinel**, implemented as a lightweight
router that always gives you a live connection to the *current master node*.

Sentinel mode does **not** hide a client behind extra abstractions —  
you get a normal `RedisClient` and can use the entire high-level API:
`get/set`, hashes, sets, sorted sets, reflection helpers, RedisBus, Pub/Sub, etc.

## Components

- `resolve_master_from_sentinel(cfg)`  
  Resolves the master address using  
  `SENTINEL get-master-addr-by-name`.

- `RedisSentinelPool`  
  Tracks the master, reconnects when failover happens, and returns a **real `RedisClient`**.

## Key Features

- Automatic master resolution from Sentinel
- Lazy connection to the master node
- Clean API:
  - `get_master_client()` → returns `shared_ptr<RedisClient>`
  - `command(...)` → sugar over the above
- Auto-reconnect on I/O errors
- Re-resolve and retry failed commands once
- Works with **all** uRedis high-level helpers (Reflect, RedisBus, etc.)
- No hidden pooling unless explicitly requested

## Example: Basic Usage

```cpp
RedisSentinelConfig cfg;
cfg.master_name = "mymaster";
cfg.sentinels = {
    {"127.0.0.1", 26379},
};

RedisSentinelPool sp{cfg};
co_await sp.connect();

// get a real RedisClient bound to the master
auto client_res = co_await sp.get_master_client();
if (!client_res)
    co_return;

auto client = client_res.value();

co_await client->set("counter", "0");

auto r = co_await client->incrby("counter", 1);
if (r)
{
    usub::ulog::info("counter = {}", r.value());
}
```

## Example: Using the command router

```cpp
RedisSentinelConfig cfg;
cfg.master_name = "mymaster";
cfg.sentinels = {
    {"127.0.0.1", 26379},
};

RedisSentinelPool sp{cfg};
co_await sp.connect();

auto v = co_await sp.command("INCRBY", "counter", "1");
if (v && v->is_integer()) {
    usub::ulog::info("counter = {}", v->as_integer());
}
```

## Example: Working with reflection

```cpp
#include "uredis/RedisReflect.h"

struct User {
    int64_t id;
    std::string name;
    bool active;
};

task::Awaitable<void> sentinel_reflect_example()
{
    RedisSentinelConfig cfg;
    cfg.master_name = "mymaster";
    cfg.sentinels = {
        {"127.0.0.1", 26379},
    };

    RedisSentinelPool sp{cfg};
    co_await sp.connect();

    auto client_res = co_await sp.get_master_client();
    if (!client_res)
        co_return;

    auto client = client_res.value();

    User u{42, "Kirill", true};

    using namespace usub::uredis::reflect;

    co_await hset_struct(*client, "user:42", u);

    auto loaded = co_await hget_struct<User>(*client, "user:42");
    if (loaded && loaded->has_value()) {
        const auto& user = **loaded;
        usub::ulog::info("Loaded user: {} {}", user.id, user.name);
    }

    co_return;
}
```

## Summary

Sentinel integration in uRedis provides:

* Minimal abstraction
* Actual `RedisClient` access
* Automatic failover handling
* Full compatibility with all uRedis features

It is designed to match `RedisClusterClient` API style, but focused on **master election** rather than sharded routing.

## Timeouts

The default master command timeout is taken from `cfg.base_redis.command_timeout_ms` (see [client.md](client.md)). For per-call control, `RedisSentinelPool` exposes `command_timed`:

```cpp
auto r = co_await sp.command_timed(1000, "GET", "key");
if (!r && r.error().category == RedisErrorCategory::Timeout) {
    // returned within 1s
}
```

When `command_timed` (or any command using `command_timeout_ms`) hits the budget, the call returns `RedisErrorCategory::Timeout` and the cached master is dropped. The next call re-resolves the master through the sentinels — useful when a timeout indicates a stale master after failover.