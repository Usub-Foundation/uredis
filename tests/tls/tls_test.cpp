// TLS integration test: needs a Redis with TLS. tests/tls/run.sh starts one in Docker and sets:
//   UREDIS_TEST_HOST           host of the server (default 127.0.0.1)
//   UREDIS_TEST_PLAIN_PORT     plain TCP port
//   UREDIS_TEST_TLS_PORT       TLS port
//   UREDIS_TEST_CA             CA bundle that signed the server certificate
//   UREDIS_TEST_SERVER_NAME    name in the server certificate
//   UREDIS_TEST_CLIENT_CERT / UREDIS_TEST_CLIENT_KEY   client certificate (optional)
// Without UREDIS_TEST_TLS_PORT the test is skipped (exit code 77).
#include <cstdio>
#include <cstdlib>
#include <string>

#include "uvent/Uvent.h"
#include "uvent/system/SystemContext.h"
#include "uredis/RedisClient.h"
#include "uredis/RedisClusterClient.h"
#include "uredis/RedisPool.h"
#include "uredis/RedisSubscriber.h"

using namespace usub::uredis;
namespace task = usub::uvent::task;
namespace system_ = usub::uvent::system;

namespace {
    int g_failed = 0;
    int g_passed = 0;
    usub::Uvent *g_runtime = nullptr;

    void check(bool ok, const std::string &name, const std::string &detail = {}) {
        if (ok) {
            ++g_passed;
            std::printf("  ok   %s\n", name.c_str());
        } else {
            ++g_failed;
            std::printf("  FAIL %s %s\n", name.c_str(), detail.c_str());
        }
        std::fflush(stdout);
    }

    std::string env(const char *name, const char *def = "") {
        const char *v = std::getenv(name);
        return v ? v : def;
    }

    RedisConfig base(bool tls) {
        RedisConfig c;
        c.host = env("UREDIS_TEST_HOST", "127.0.0.1");
        c.port = static_cast<std::uint16_t>(std::stoi(env(tls ? "UREDIS_TEST_TLS_PORT" : "UREDIS_TEST_PLAIN_PORT")));
        c.io_timeout_ms = 3000;
        if (tls) {
            c.tls.enabled = true;
            c.tls.ca_file = env("UREDIS_TEST_CA");
            c.tls.server_name = env("UREDIS_TEST_SERVER_NAME");
        }
        return c;
    }

    std::string err_of(const RedisError &e) { return "(" + e.message + ")"; }

    task::Awaitable<void> client_roundtrip(std::string label, RedisConfig cfg) {
        RedisClient c{cfg};
        auto con = co_await c.connect();
        check(con.has_value(), label + ": connect", con ? "" : err_of(con.error()));
        if (!con) co_return;

        auto set = co_await c.set("uredis:tls:k", label);
        check(set.has_value(), label + ": SET");
        auto get = co_await c.get("uredis:tls:k");
        check(get && *get && **get == label, label + ": GET returns the value");

        // 1 MiB value: many TLS records each way.
        std::string big(1 << 20, 'x');
        for (std::size_t i = 0; i < big.size(); i += 4093) big[i] = static_cast<char>('a' + (i % 26));
        auto bset = co_await c.set("uredis:tls:big", big);
        auto bget = co_await c.get("uredis:tls:big");
        check(bset && bget && *bget && **bget == big, label + ": 1 MiB value round-trips");

        bool all = true;
        for (int i = 0; i < 300 && all; ++i) {
            auto r = co_await c.command("INCR", std::string_view{"uredis:tls:ctr"});
            all = r.has_value() && r->is_integer();
        }
        check(all, label + ": 300 sequential commands");
    }

    // PING after connect; co_await kept out of conditional expressions (GCC coroutine bugs).
    task::Awaitable<RedisResult<RedisValue> > connect_and_ping(RedisClient &c) {
        auto con = co_await c.connect();
        if (!con) co_return std::unexpected(con.error());
        co_return co_await c.command("PING");
    }

    task::Awaitable<void> run_all() {
        std::printf("== plain TCP (TLS disabled, previous behavior)\n");
        co_await client_roundtrip("plain", base(false));

        std::printf("== TLS\n");
        co_await client_roundtrip("tls", base(true));

        {
            auto cfg = base(true);
            cfg.tls.server_name = "wrong.example";
            RedisClient c{cfg};
            auto con = co_await c.connect();
            check(!con.has_value() && con.error().category == RedisErrorCategory::Io,
                  "tls: wrong server name is rejected", con ? "connected!" : err_of(con.error()));
        }
        {
            auto cfg = base(true);
            cfg.tls.ca_file.reset();  // system store does not know the test CA
            RedisClient c{cfg};
            auto con = co_await c.connect();
            check(!con.has_value(), "tls: unknown CA is rejected", con ? "connected!" : err_of(con.error()));
        }
        {
            auto cfg = base(true);
            cfg.tls.verify_peer = false;
            cfg.tls.ca_file.reset();
            cfg.tls.server_name = "wrong.example";
            RedisClient c{cfg};
            auto pong = co_await connect_and_ping(c);
            check(pong.has_value(), "tls: verify_peer=false connects (encryption only)",
                  pong ? "" : err_of(pong.error()));
        }
        {
            auto cfg = base(false);
            cfg.tls.enabled = true;  // TLS client against the plain port
            cfg.tls.ca_file = env("UREDIS_TEST_CA");
            cfg.io_timeout_ms = 1000;
            RedisClient c{cfg};
            auto con = co_await c.connect();
            check(!con.has_value(), "tls: plain-text server is rejected", con ? "connected!" : err_of(con.error()));
        }
        {
            auto cfg = base(true);
            cfg.tls.ca_file = "/nonexistent/ca.pem";
            RedisClient c{cfg};
            auto con = co_await c.connect();
            check(!con.has_value(), "tls: missing CA file fails cleanly", con ? "connected!" : err_of(con.error()));
        }
        if (!env("UREDIS_TEST_CLIENT_CERT").empty()) {
            auto cfg = base(true);
            cfg.tls.cert_file = env("UREDIS_TEST_CLIENT_CERT");
            cfg.tls.key_file = env("UREDIS_TEST_CLIENT_KEY");
            RedisClient c{cfg};
            auto pong = co_await connect_and_ping(c);
            check(pong.has_value(), "tls: client certificate (mTLS)", pong ? "" : err_of(pong.error()));
        }

        {
            RedisClusterConfig cc;
            auto b = base(true);
            cc.seeds.push_back({.host = b.host, .port = b.port});
            cc.force_standalone = true;
            cc.io_timeout_ms = 3000;
            cc.tls = b.tls;
            RedisClusterClient cluster{cc};
            auto con = co_await cluster.connect();
            check(con.has_value(), "tls: cluster client (standalone) connect", con ? "" : err_of(con.error()));
            if (con) {
                auto s = co_await cluster.command("SET", std::string_view{"uredis:tls:cl"}, std::string_view{"1"});
                auto g = co_await cluster.command("GET", std::string_view{"uredis:tls:cl"});
                check(s && g && g->is_bulk_string() && g->as_string() == "1", "tls: cluster client SET/GET");
            }
        }
        {
            RedisPoolConfig pc;
            auto b = base(true);
            pc.host = b.host;
            pc.port = b.port;
            pc.size = 2;
            pc.io_timeout_ms = 3000;
            pc.tls = b.tls;
            RedisPool pool{pc};
            auto con = co_await pool.connect_all();
            check(con.has_value(), "tls: pool connect", con ? "" : err_of(con.error()));
            if (con) {
                auto r = co_await pool.command("PING");
                check(r.has_value(), "tls: pool PING");
            }
        }
        {
            RedisSubscriber sub{base(true)};
            auto con = co_await sub.connect();
            check(con.has_value(), "tls: subscriber connect", con ? "" : err_of(con.error()));
            if (con) {
                auto got = std::make_shared<std::string>();
                auto s = co_await sub.subscribe("uredis:tls:ch", [got](const std::string &, const std::string &payload) {
                    *got = payload;
                });
                check(s.has_value(), "tls: SUBSCRIBE");
                RedisClient pub{base(true)};
                auto pc = co_await pub.connect();
                if (pc) co_await pub.command("PUBLISH", std::string_view{"uredis:tls:ch"}, std::string_view{"hello-tls"});
                for (int i = 0; i < 50 && got->empty(); ++i)
                    if (!co_await system_::this_coroutine::sleep_for(std::chrono::milliseconds{20})) break;
                check(*got == "hello-tls", "tls: message delivered to subscriber", "got='" + *got + "'");
                co_await sub.close();
            }
        }

        std::printf("passed=%d failed=%d\n", g_passed, g_failed);
        g_runtime->stop();
        co_return;
    }
} // namespace

int main() {
    if (env("UREDIS_TEST_TLS_PORT").empty() || env("UREDIS_TEST_PLAIN_PORT").empty()) {
        std::printf("skip: UREDIS_TEST_TLS_PORT / UREDIS_TEST_PLAIN_PORT not set (see tests/tls/run.sh)\n");
        return 77;
    }
    if (!tls_supported()) {
        std::printf("FAIL: uredis built without TLS support\n");
        return 1;
    }
    usub::Uvent runtime{1};
    g_runtime = &runtime;
    system_::co_spawn_static(run_all(), 0);
    runtime.run();
    return g_failed == 0 && g_passed > 0 ? 0 : 1;
}
