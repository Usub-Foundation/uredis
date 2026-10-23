// RedisClusterClient recovery integration test. tests/cluster/run.sh drives a single-node
// Redis Cluster in Docker on the test's request and sets:
//   UREDIS_TEST_HOST   host of the server (default 127.0.0.1)
//   UREDIS_TEST_PORT   port Redis will listen on (nothing listens there when the test starts)
//   UREDIS_TEST_CTL    control directory: the test writes req.N (start | pause | unpause),
//                      run.sh performs it and answers with ack.N
// Without UREDIS_TEST_PORT / UREDIS_TEST_CTL the test is skipped (exit code 77).
//
// What it proves:
//   1. connect() / commands recover once Redis comes up after a failed start (no cached error).
//   2. Timeouts and dead connections do not stampede CLUSTER SLOTS: concurrent failures share
//      one rediscovery, a plain command timeout triggers none.
//   3. A caller whose deadline expires while it waits for a pooled connection does not close
//      a healthy connection.
// Server-side counters (INFO: total_connections_received, cmdstat_cluster|slots) are the
// measure, so the client API needs no test hooks.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncWaitGroup.h"
#include "uvent/system/SystemContext.h"
#include "uredis/RedisClient.h"
#include "uredis/RedisClusterClient.h"

using namespace usub::uredis;
namespace task = usub::uvent::task;
namespace usync = usub::uvent::sync;
namespace system_ = usub::uvent::system;

namespace {
    int g_failed = 0;
    int g_passed = 0;
    int g_ctl_seq = 0;
    usub::Uvent *g_runtime = nullptr;

    void check(bool ok, const std::string &name, const std::string &detail = {}) {
        if (ok) {
            ++g_passed;
            std::printf("  ok   %s %s\n", name.c_str(), detail.c_str());
        } else {
            ++g_failed;
            std::printf("  FAIL %s %s\n", name.c_str(), detail.c_str());
        }
        std::fflush(stdout);
    }

    void note(const std::string &s) {
        std::printf("  ..   %s\n", s.c_str());
        std::fflush(stdout);
    }

    std::string env(const char *name, const char *def = "") {
        const char *v = std::getenv(name);
        return v ? v : def;
    }

    std::int64_t now_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    std::string err_of(const RedisError &e) { return "(" + e.message + ")"; }

    std::string host() { return env("UREDIS_TEST_HOST", "127.0.0.1"); }
    std::uint16_t port() { return static_cast<std::uint16_t>(std::stoi(env("UREDIS_TEST_PORT"))); }

    RedisClusterConfig cluster_cfg() {
        RedisClusterConfig c;
        c.seeds.push_back({.host = host(), .port = port()});
        c.connect_timeout_ms = 1000;
        c.io_timeout_ms = 3000;
        c.max_connections_per_node = 4;
        return c;
    }

    // Asks run.sh to perform `action` and waits for its acknowledgement. The file I/O is a
    // few syscalls on a local tmpfs-like dir: fine for a test, not a pattern for services.
    task::Awaitable<bool> control(std::string action) {
        const int n = ++g_ctl_seq;
        const std::string dir = env("UREDIS_TEST_CTL");
        const std::string req = dir + "/req." + std::to_string(n);
        {
            std::ofstream f(req + ".tmp");
            f << action << "\n";
        }
        std::filesystem::rename(req + ".tmp", req);
        const std::string ack = dir + "/ack." + std::to_string(n);
        for (int i = 0; i < 1200; ++i) { // 60 s
            if (std::filesystem::exists(ack)) {
                note("run.sh: " + action + " done");
                co_return true;
            }
            if (!co_await system_::this_coroutine::sleep_for(std::chrono::milliseconds{50}))
                co_return false;
        }
        co_return false;
    }

    struct ServerStats {
        long long conns{-1};         // total_connections_received
        long long cluster_slots{-1}; // CLUSTER SLOTS calls
    };

    long long info_field(const std::string &text, const std::string &key) {
        auto p = text.find(key);
        if (p == std::string::npos) return -1;
        p += key.size();
        auto e = text.find_first_not_of("0123456789", p);
        return std::stoll(text.substr(p, e - p));
    }

    task::Awaitable<ServerStats> server_stats(RedisClient *c) {
        ServerStats s;
        auto st = co_await c->command("INFO", std::string_view{"stats"});
        if (!st || !st->is_bulk_string()) co_return s;
        s.conns = info_field(st->as_string(), "total_connections_received:");
        auto cs = co_await c->command("INFO", std::string_view{"commandstats"});
        if (!cs || !cs->is_bulk_string()) co_return s;
        const auto v = info_field(cs->as_string(), "cmdstat_cluster|slots:calls=");
        s.cluster_slots = v < 0 ? 0 : v;
        co_return s;
    }

    // Baseline after the server has accepted every connection the client just opened
    // (a prewarmed pool connects without a round-trip, Redis accepts asynchronously).
    task::Awaitable<ServerStats> settled_stats(RedisClient *c) {
        if (!co_await system_::this_coroutine::sleep_for(std::chrono::milliseconds{200}))
            co_return ServerStats{};
        co_return co_await server_stats(c);
    }

    // Outcome of a burst of concurrent calls.
    struct Batch {
        usync::WaitGroup wg;
        std::atomic<int> ok{0};
        std::atomic<int> timeouts{0};
        std::atomic<int> other{0};
        std::atomic<std::int64_t> max_ms{0};
        std::mutex mu;
        std::string first_other;

        void record(const RedisResult<void> &r, std::int64_t dt) {
            std::int64_t cur = max_ms.load();
            while (dt > cur && !max_ms.compare_exchange_weak(cur, dt)) {
            }
            if (r) {
                ++ok;
            } else if (r.error().category == RedisErrorCategory::Timeout) {
                ++timeouts;
            } else {
                ++other;
                std::lock_guard<std::mutex> lk(mu);
                if (first_other.empty()) first_other = r.error().message;
            }
        }

        std::string summary() const {
            return "ok=" + std::to_string(ok.load()) + " timeout=" + std::to_string(timeouts.load()) +
                   " other=" + std::to_string(other.load()) + " max_ms=" + std::to_string(max_ms.load()) +
                   (first_other.empty() ? "" : " first_other=(" + first_other + ")");
        }
    };

    // Detached workers: state only through parameters (no captures), exceptions caught.
    task::Awaitable<void> one_get(RedisClusterClient *cl, std::shared_ptr<Batch> b, std::string key) {
        try {
            const auto t0 = now_ms();
            auto r = co_await cl->command("GET", std::string_view{key});
            RedisResult<void> v{};
            if (!r) v = std::unexpected(r.error());
            b->record(v, now_ms() - t0);
        } catch (...) {
            b->record(std::unexpected(RedisError{RedisErrorCategory::Protocol, "exception"}), 0);
        }
        b->wg.done();
    }

    task::Awaitable<void> one_connect(RedisClusterClient *cl, std::shared_ptr<Batch> b) {
        try {
            const auto t0 = now_ms();
            auto r = co_await cl->connect();
            b->record(r, now_ms() - t0);
        } catch (...) {
            b->record(std::unexpected(RedisError{RedisErrorCategory::Protocol, "exception"}), 0);
        }
        b->wg.done();
    }

    task::Awaitable<void> one_blpop(RedisClusterClient *cl, std::shared_ptr<Batch> b) {
        try {
            const auto t0 = now_ms();
            auto r = co_await cl->command_timed(5000, "BLPOP", std::string_view{"uredis:rt:blk"},
                                                std::string_view{"1"});
            RedisResult<void> v{};
            if (!r) v = std::unexpected(r.error());
            b->record(v, now_ms() - t0);
        } catch (...) {
            b->record(std::unexpected(RedisError{RedisErrorCategory::Protocol, "exception"}), 0);
        }
        b->wg.done();
    }

    task::Awaitable<std::shared_ptr<Batch> > get_burst(RedisClusterClient *cl, int n, std::string prefix) {
        auto b = std::make_shared<Batch>();
        b->wg.add(n);
        for (int i = 0; i < n; ++i)
            system_::co_spawn(one_get(cl, b, prefix + std::to_string(i)));
        co_await b->wg.wait();
        co_return b;
    }

    // ---- 1. start while Redis is down, recover without recreating the client
    task::Awaitable<bool> phase_down_then_up(RedisClient *stats) {
        std::printf("== 1. Redis down at start, then up (no cached connect error)\n");
        auto cfg = cluster_cfg();
        RedisClusterClient a{cfg};
        RedisClusterClient b{cfg}; // never connect()-ed explicitly

        auto c1 = co_await a.connect();
        check(!c1.has_value(), "down: connect() fails while nothing listens", c1 ? "connected!" : err_of(c1.error()));
        auto c2 = co_await a.connect();
        check(!c2.has_value(), "down: repeated connect() fails as well");
        {
            auto burst = std::make_shared<Batch>();
            burst->wg.add(20);
            for (int i = 0; i < 20; ++i)
                system_::co_spawn(one_connect(&a, burst));
            co_await burst->wg.wait();
            check(burst->ok == 0 && burst->other + burst->timeouts == 20,
                  "down: 20 concurrent connect() all fail, none hangs", burst->summary());
        }
        auto s1 = co_await b.command("SET", std::string_view{"uredis:rt:k"}, std::string_view{"v1"});
        check(!s1.has_value(), "down: command() before any successful connect fails",
              s1 ? "succeeded!" : err_of(s1.error()));

        if (!co_await control("start")) {
            check(false, "run.sh did not start Redis");
            co_return false;
        }
        auto sc = co_await stats->connect();
        if (!sc) {
            check(false, "stats connection", err_of(sc.error()));
            co_return false;
        }

        auto c3 = co_await a.connect();
        check(c3.has_value(), "up: connect() on the SAME client succeeds now", c3 ? "" : err_of(c3.error()));
        auto set = co_await a.command("SET", std::string_view{"uredis:rt:k"}, std::string_view{"v2"});
        auto get = co_await a.command("GET", std::string_view{"uredis:rt:k"});
        check(set && get && get->is_bulk_string() && get->as_string() == "v2", "up: SET/GET through the same client");

        auto g2 = co_await b.command("GET", std::string_view{"uredis:rt:k"});
        check(g2 && g2->is_bulk_string() && g2->as_string() == "v2",
              "up: command() alone recovers a client whose only discovery failed", g2 ? "" : err_of(g2.error()));

        // Concurrent first use of a fresh client: one discovery for everybody.
        auto st0 = co_await server_stats(stats);
        {
            RedisClusterClient c{cfg};
            auto burst = std::make_shared<Batch>();
            burst->wg.add(50);
            for (int i = 0; i < 25; ++i) {
                system_::co_spawn(one_connect(&c, burst));
                system_::co_spawn(one_get(&c, burst, "uredis:rt:k" + std::to_string(i)));
            }
            co_await burst->wg.wait();
            auto st1 = co_await server_stats(stats);
            check(burst->ok == 50, "up: 25 connect() + 25 GET concurrently on a fresh client", burst->summary());
            const auto slots = st1.cluster_slots - st0.cluster_slots;
            check(slots == 1, "up: they share ONE discovery", "CLUSTER SLOTS calls=" + std::to_string(slots));
        }
        co_return true;
    }

    // ---- 3. deadline expires while waiting for the only pooled connection
    task::Awaitable<void> phase_pool_wait(RedisClient *stats) {
        std::printf("== 3. deadline passes while waiting for a pooled connection\n");
        auto cfg = cluster_cfg();
        cfg.max_connections_per_node = 1;
        RedisClusterClient d{cfg};
        auto con = co_await d.connect();
        check(con.has_value(), "pool-wait: connect", con ? "" : err_of(con.error()));
        if (!con) co_return;
        auto warm = co_await d.command("GET", std::string_view{"uredis:rt:k"});
        check(warm.has_value(), "pool-wait: warm-up GET");

        auto st0 = co_await settled_stats(stats);
        auto blocker = std::make_shared<Batch>();
        blocker->wg.add(1);
        system_::co_spawn(one_blpop(&d, blocker)); // holds the only connection for ~1 s
        if (!co_await system_::this_coroutine::sleep_for(std::chrono::milliseconds{150})) {
            co_await blocker->wg.wait();
            co_return;
        }

        const auto t0 = now_ms();
        auto r = co_await d.command_timed(200, "GET", std::string_view{"uredis:rt:k"});
        const auto dt = now_ms() - t0;
        check(!r && r.error().category == RedisErrorCategory::Timeout,
              "pool-wait: GET with a 200 ms budget times out", r ? "succeeded?" : err_of(r.error()));
        check(dt < 700, "pool-wait: ... within its budget, not when the busy connection frees up",
              "took " + std::to_string(dt) + " ms");

        co_await blocker->wg.wait();
        check(blocker->ok == 1, "pool-wait: the BLPOP holding the connection completed", blocker->summary());
        auto after = co_await d.command("GET", std::string_view{"uredis:rt:k"});
        check(after.has_value(), "pool-wait: next GET succeeds", after ? "" : err_of(after.error()));

        auto st1 = co_await server_stats(stats);
        const auto conns = st1.conns - st0.conns;
        const auto slots = st1.cluster_slots - st0.cluster_slots;
        check(conns == 0, "pool-wait: the healthy connection was kept (no reconnect)",
              "new connections=" + std::to_string(conns));
        check(slots == 0, "pool-wait: no topology rebuild", "CLUSTER SLOTS calls=" + std::to_string(slots));
    }

    // ---- 2a. command timeouts while Redis is frozen: no rediscovery at all
    task::Awaitable<void> phase_pause_timeouts(RedisClient *stats) {
        std::printf("== 2a. Redis frozen (docker pause), commands hit command_timeout_ms\n");
        auto cfg = cluster_cfg();
        cfg.command_timeout_ms = 300;
        cfg.io_timeout_ms = 3000;
        RedisClusterClient e{cfg};
        auto con = co_await e.connect();
        check(con.has_value(), "pause/timeout: connect", con ? "" : err_of(con.error()));
        if (!con) co_return;

        auto st0 = co_await settled_stats(stats);
        if (!co_await control("pause")) {
            check(false, "run.sh did not pause Redis");
            co_return;
        }
        auto b1 = co_await get_burst(&e, 100, "uredis:rt:p");
        if (!co_await control("unpause")) {
            check(false, "run.sh did not unpause Redis");
            co_return;
        }
        check(b1->ok == 0 && b1->timeouts == 100, "pause/timeout: 100 concurrent GETs time out", b1->summary());
        check(b1->max_ms < 1000, "pause/timeout: ... close to the 300 ms budget",
              "max " + std::to_string(b1->max_ms.load()) + " ms");

        auto b2 = co_await get_burst(&e, 100, "uredis:rt:p");
        check(b2->ok == 100, "pause/timeout: after unpause 100 concurrent GETs succeed", b2->summary());

        auto st1 = co_await server_stats(stats);
        const auto conns = st1.conns - st0.conns;
        const auto slots = st1.cluster_slots - st0.cluster_slots;
        check(slots == 0, "pause/timeout: no CLUSTER SLOTS rediscovery from timeouts",
              "CLUSTER SLOTS calls=" + std::to_string(slots));
        // 4 pooled connections closed by timed-out in-flight commands + a few opened by
        // waiters + the pool refilled after unpause.
        check(conns <= 16, "pause/timeout: connection churn stays bounded",
              "new connections=" + std::to_string(conns));
    }

    // ---- 2b. connections die (IO timeout) concurrently: one shared rediscovery
    task::Awaitable<void> phase_pause_io(RedisClient *stats) {
        std::printf("== 2b. Redis frozen, every pooled connection fails with an IO error at once\n");
        auto cfg = cluster_cfg();
        cfg.io_timeout_ms = 300;
        cfg.command_timeout_ms = 0;
        cfg.max_connections_per_node = 32;
        RedisClusterClient f{cfg};
        auto con = co_await f.connect(); // prewarms 32 connections
        check(con.has_value(), "pause/io: connect", con ? "" : err_of(con.error()));
        if (!con) co_return;

        auto st0 = co_await settled_stats(stats);
        if (!co_await control("pause")) {
            check(false, "run.sh did not pause Redis");
            co_return;
        }
        const auto t0 = now_ms();
        auto b1 = co_await get_burst(&f, 32, "uredis:rt:q");
        const auto dt = now_ms() - t0;
        if (!co_await control("unpause")) {
            check(false, "run.sh did not unpause Redis");
            co_return;
        }
        check(b1->ok == 0 && b1->other == 32, "pause/io: 32 concurrent GETs fail with IO errors", b1->summary());
        // Each GET: read timeout, RedisClient reconnect + resend, read timeout (2 x 300 ms), then
        // one rediscovery whose CLUSTER SLOTS fails the same way (2 x 300 ms). Shared, that is
        // ~1.2 s in total; serialized per caller it would be 32 x 0.6 s.
        check(dt < 4000, "pause/io: the burst finishes quickly (rediscovery shared)",
              "took " + std::to_string(dt) + " ms");

        auto b2 = co_await get_burst(&f, 32, "uredis:rt:q");
        check(b2->ok == 32, "pause/io: after unpause 32 concurrent GETs succeed", b2->summary());

        auto st1 = co_await server_stats(stats);
        const auto slots = st1.cluster_slots - st0.cluster_slots;
        // One rediscovery run sends CLUSTER SLOTS twice (RedisClient retries once on IO error).
        check(slots <= 4, "pause/io: concurrent failures share one rediscovery run",
              "CLUSTER SLOTS calls=" + std::to_string(slots) + " (1 run = 2 calls)");
        note("pause/io: new connections=" + std::to_string(st1.conns - st0.conns));
    }

    task::Awaitable<void> run_all() {
        RedisConfig sc;
        sc.host = host();
        sc.port = port();
        sc.io_timeout_ms = 10000;
        RedisClient stats{sc};

        if (co_await phase_down_then_up(&stats)) {
            co_await phase_pool_wait(&stats);
            co_await phase_pause_timeouts(&stats);
            co_await phase_pause_io(&stats);
        }

        std::printf("passed=%d failed=%d\n", g_passed, g_failed);
        std::fflush(stdout);
        g_runtime->stop();
        co_return;
    }
} // namespace

int main() {
    if (env("UREDIS_TEST_PORT").empty() || env("UREDIS_TEST_CTL").empty()) {
        std::printf("skip: UREDIS_TEST_PORT / UREDIS_TEST_CTL not set (see tests/cluster/run.sh)\n");
        return 77;
    }
    usub::Uvent runtime{2};
    g_runtime = &runtime;
    system_::co_spawn_static(run_all(), 0);
    runtime.run();
    return g_failed == 0 && g_passed > 0 ? 0 : 1;
}
