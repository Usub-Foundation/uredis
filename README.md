# uRedis

uRedis is a small, async-first Redis client library built on top of
[uvent](https://github.com/Usub-development/uvent).

It provides:

- `RedisClient` – single async connection with RESP parsing and typed helpers.
- `RedisPool` – round-robin pool of multiple RedisClient instances.
- `RedisSubscriber` – low-level SUBSCRIBE / PSUBSCRIBE client.
- `RedisBus` – high-level resilient pub/sub bus with auto-reconnect and resubscription.
- `RedisValue` / `RedisResult` / `RedisError` – result and error types.
- `RespParser` – incremental RESP parser, fully implemented in C++.
- `reflect` helpers – map C++ aggregates to Redis hashes (`HSET` / `HGETALL`) using **ureflect**.
- (optional) **Sentinel support** – resolve current master, auto-reconnect, retrieve a real `RedisClient`.
- (optional) **Cluster client** – route commands by hash slot, get a real `RedisClient` bound to a slot.
- (optional) **Redlock** – distributed locks across multiple Redis instances.

Documentation: **https://usub-development.github.io/uredis/**

---

## Features

- Async coroutine-based API (`task::Awaitable<>`)
- Zero external Redis dependencies (no hiredis)
- Clean error model:
    - `Io` – connection / timeout / read/write error
    - `Protocol` – malformed reply or unexpected type
    - `ServerReply` – `-ERR ...` from Redis
- Pub/Sub:
    - raw `RedisSubscriber`
    - higher-level `RedisBus` with:
        - dedicated pub/sub connections
        - periodic `PING`
        - reconnect + resubscribe loop
- Sentinel:
    - resolve master via `SENTINEL get-master-addr-by-name`
    - lazily connect to current master
    - `get_master_client()` → full RedisClient API
    - automatic re-resolve on I/O errors
- Cluster:
    - CRC16 hashing
    - MOVED and ASK redirection
    - auto-discovery with `CLUSTER SLOTS`
    - `get_client_for_key()` → real RedisClient for the slot
- Reflection helpers compatible with single client, Sentinel, and Cluster
- Optional TLS (OpenSSL): server verification, SNI, custom CA, client certificates; off by
  default — see [docs/tls.md](docs/tls.md)

---

## Requirements

- C++23
- [uvent](https://github.com/Usub-development/uvent)
- [ulog](https://github.com/Usub-development/ulog) *(optional, for logging)*
- [ureflect](https://github.com/Usub-development/ureflect) *(for reflection helpers)*

Redis 6+ recommended (RESP3 works but RESP2 is fully supported).

---

## Building / Integration

### CMake FetchContent example

```cmake
include(FetchContent)

FetchContent_Declare(
        uvent
        GIT_REPOSITORY https://github.com/Usub-development/uvent.git
        GIT_TAG main
)

FetchContent_Declare(
        ureflect
        GIT_REPOSITORY https://github.com/Usub-development/ureflect.git
        GIT_TAG main
)

FetchContent_Declare(
        uredis
        GIT_REPOSITORY https://github.com/Usub-development/uredis.git
        GIT_TAG main
)

FetchContent_MakeAvailable(uvent ureflect uredis)

add_executable(my_app main.cpp)

target_link_libraries(my_app
        PRIVATE
        uvent
        uredis
        ureflect
)
```

### Enable built-in logs

```cmake
target_compile_definitions(uredis PUBLIC UREDIS_LOGS)
```

Logs are emitted through `ulog`.

---

## Quick start

### Single client

```cpp
#include "uvent/Uvent.h"
#include "uredis/RedisClient.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;
namespace task = usub::uvent::task;

task::Awaitable<void> example_single()
{
    RedisConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 15100;

    RedisClient client{cfg};
    auto c = co_await client.connect();
    if (!c)
    {
        usub::ulog::error("connect failed: {}", c.error().message);
        co_return;
    }

    co_await client.set("foo", "bar");

    auto g = co_await client.get("foo");
    if (g && g.value().has_value())
        usub::ulog::info("foo = {}", *g.value());

    co_return;
}

int main()
{
    usub::ulog::ULogInit log_cfg{ .enable_color_stdout = true };
    usub::ulog::init(log_cfg);

    Uvent uvent(4);
    system::co_spawn(example_single());
    uvent.run();
}
```

---

## Pool example

```cpp
task::Awaitable<void> example_pool()
{
    RedisPoolConfig pcfg;
    pcfg.host = "127.0.0.1";
    pcfg.port = 15100;
    pcfg.size = 8;

    RedisPool pool{pcfg};
    co_await pool.connect_all();

    auto r = co_await pool.command("INCRBY", "counter", "1");
    if (r && r->is_integer())
        usub::ulog::info("counter = {}", r->as_integer());

    co_return;
}
```

---

## Sentinel example (new API)

Sentinel returns a **real RedisClient** bound to the current master.

```cpp
#include "uredis/RedisSentinelPool.h"

task::Awaitable<void> example_sentinel()
{
    RedisSentinelConfig cfg;
    cfg.master_name = "mymaster";
    cfg.sentinels = { {"127.0.0.1", 26379} };

    RedisSentinelPool sp{cfg};
    co_await sp.connect();

    auto client_res = co_await sp.get_master_client();
    if (!client_res)
        co_return;

    auto client = client_res.value();
    co_await client->set("mkey", "123");

    auto g = co_await client->get("mkey");
    if (g && g->has_value())
        usub::ulog::info("master says: {}", **g);

    co_return;
}
```

---

## Cluster example (new API)

Cluster client also returns a **real RedisClient** for the correct slot.

```cpp
#include "uredis/RedisClusterClient.h"

task::Awaitable<void> example_cluster()
{
    RedisClusterConfig cfg;
    cfg.seeds = { {"127.0.0.1", 7000}, {"127.0.0.1", 7001} };

    RedisClusterClient cluster{cfg};
    co_await cluster.connect();

    auto client_res = co_await cluster.get_client_for_key("user:42");
    if (!client_res)
        co_return;

    auto client = client_res.value();
    co_await client->set("user:42", "Kirill");

    auto g = co_await client->get("user:42");
    if (g && g->has_value())
        usub::ulog::info("user:42 = {}", **g);

    co_return;
}
```

---

## Reflection helpers

Works with:

* single RedisClient
* Sentinel (`get_master_client()`)
* Cluster (`get_client_for_key()`)

```cpp
#include "uredis/RedisReflect.h"

struct User { int64_t id; std::string name; bool active; };

using namespace usub::uredis::reflect;

auto h1 = co_await hset_struct(*client, "user:42", u);
auto h2 = co_await hget_struct<User>(*client, "user:42");
```

---

## License

uRedis is distributed under the [MIT license](LICENSE).