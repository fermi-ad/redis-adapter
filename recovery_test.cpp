#include "RedisAdapter.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <unistd.h>

using namespace std::chrono_literals;

template<class F> void eventually(F predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 4s;
  while (!predicate()) {
    assert(std::chrono::steady_clock::now() < deadline);
    std::this_thread::sleep_for(5ms);
  }
}

struct Observed {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<std::string> ids;
  void append(const RedisAdapter::StreamBatch& entries) {
    std::lock_guard<std::mutex> guard(mutex);
    for (const auto& entry : entries) ids.push_back(entry.first);
    changed.notify_all();
  }
  bool wait(const std::string& id) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 4s, [&] { return std::find(ids.begin(), ids.end(), id) != ids.end(); });
  }
  size_t count(const std::string& id) {
    std::lock_guard<std::mutex> guard(mutex);
    return std::count(ids.begin(), ids.end(), id);
  }
};

int main() {
  RA_Options options;
  if (const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT")) options.cxn.port = std::stoi(port);
  options.readerProbeMs = 50;
  sw::redis::ConnectionOptions connection;
  connection.host = "127.0.0.1"; connection.port = options.cxn.port;
  if (std::getenv("REDIS_ADAPTER_CLUSTER_TEST")) {
    sw::redis::RedisCluster control(connection);
    for (const auto& base : {"a", "b", "c"}) {
      const auto key = "{" + std::string(base) + "}:value";
      const RedisAdapter::Attrs fields{{"_", "cluster-value"}};
      control.xadd(key, "1000-0", fields.begin(), fields.end());
      RedisAdapter adapter(base, options);
      Observed observed;
      auto handle = adapter.subscribeStream("value", [&](const auto&, const auto&, const auto& entries) {
        observed.append(entries);
      }, "0-0");
      assert(observed.wait("1000-0") && handle.status().inspected);
      control.del(key); control.xadd(key, "1-0", fields.begin(), fields.end());
      assert(observed.wait("1-0") && handle.status().streamResets == 1);
      assert(handle.status().inspectionRejections == 0);
    }
    std::cout << "cluster-routed inspection and reset recovery passed\n";
    return 0;
  }
  sw::redis::Redis control(connection);
  const auto base = "stream-recovery-" + std::to_string(getpid());
  const auto key = "{" + base + "}:value";
  const RedisAdapter::Attrs fields{{"_", "value"}};
  control.xadd(key, "1000-0", fields.begin(), fields.end());
  RedisAdapter adapter(base, options);
  Observed observed;
  auto handle = adapter.subscribeStream("value", [&](const auto&, const auto&, const auto& entries) {
    observed.append(entries);
  }, "0-0");
  assert(observed.wait("1000-0"));
  control.del(key);
  control.xadd(key, "1-0", fields.begin(), fields.end());
  assert(observed.wait("1-0") && "recreated stream must resume below the previous cursor");
  assert(handle.status().streamResets == 1 && handle.status().epoch == 2);
  control.xadd(key, "2-0", fields.begin(), fields.end());
  assert(observed.wait("2-0"));
  assert(handle.status().cursor == "2-0" && handle.status().observedCursor == "2-0");

  control.del(key);
  eventually([&] { return handle.status().stream == RedisConnection::StreamKind::Missing; });
  assert(handle.status().disappearances == 1 && !handle.status().hasData);
  control.xadd(key, "3-0", fields.begin(), fields.end());
  assert(observed.wait("3-0"));

  // A wrong-type key must not stall other readers in the same bucket.
  Observed healthy;
  auto other = adapter.subscribeStream("healthy", [&](const auto&, const auto&, const auto& entries) {
    healthy.append(entries);
  }, "0-0");
  control.del(key); control.set(key, "wrong-type");
  eventually([&] { return handle.status().stream == RedisConnection::StreamKind::Invalid; });
  control.xadd("{" + base + "}:healthy", "10-0", fields.begin(), fields.end());
  assert(healthy.wait("10-0"));
  control.del(key); control.xadd(key, "4-0", fields.begin(), fields.end());
  assert(observed.wait("4-0"));

  // Connection loss preserves cursors and cannot duplicate prior callbacks.
  const auto failures = handle.status().readFailures + handle.status().inspectionFailures;
  const auto resets = handle.status().streamResets;
  control.command<long long>("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes");
  eventually([&] { const auto status = handle.status(); return status.readFailures + status.inspectionFailures > failures; });
  eventually([&] { return handle.status().connected && handle.status().reconnects > 0; });
  control.xadd(key, "5-0", fields.begin(), fields.end());
  assert(observed.wait("5-0"));
  assert(observed.count("4-0") == 1 && handle.status().streamResets == resets);

  // A cursor below retained history is a possible gap, not a fabricated count
  // of missed records. Exact trim keeps the latest stream ID intact.
  const auto trimmed = "{" + base + "}:trimmed";
  control.xadd(trimmed, "10-0", fields.begin(), fields.end());
  control.xadd(trimmed, "20-0", fields.begin(), fields.end());
  control.xtrim(trimmed, 1, false);
  Observed retained;
  auto trim = adapter.subscribeStream("trimmed", [&](const auto&, const auto&, const auto& entries) {
    retained.append(entries);
  }, "10-0");
  assert(retained.wait("20-0"));
  assert(trim.status().retentionGaps == 1 && trim.status().streamResets == 0);
  control.xtrim(trimmed, 0, false);
  eventually([&] { return !trim.status().hasData; });
  assert(trim.status().streamResets == 0 && trim.status().cursor == "20-0");
  control.xadd(trimmed, "30-0", fields.begin(), fields.end());
  assert(retained.wait("30-0"));

  // Fence data already queued from the old stream while a worker is busy.
  std::mutex mutex;
  std::condition_variable changed;
  bool entered = false, release = false;
  auto blocker = adapter.subscribeStream("blocker", [&](const auto&, const auto&, const auto&) {
    std::unique_lock<std::mutex> lock(mutex);
    entered = true; changed.notify_all();
    assert(changed.wait_for(lock, 10s, [&] { return release; }));
  }, "0-0");
  control.xadd("{" + base + "}:blocker", "1-0", fields.begin(), fields.end());
  { std::unique_lock<std::mutex> lock(mutex); assert(changed.wait_for(lock, 4s, [&] { return entered; })); }
  const auto fencedKey = "{" + base + "}:fenced";
  control.xadd(fencedKey, "1000-0", fields.begin(), fields.end());
  Observed fenced;
  auto queued = adapter.subscribeStream("fenced", [&](const auto&, const auto&, const auto& entries) {
    fenced.append(entries);
  }, "0-0");
  eventually([&] { return queued.status().observedCursor == "1000-0"; });
  control.del(fencedKey); control.xadd(fencedKey, "1-0", fields.begin(), fields.end());
  eventually([&] { return queued.status().streamResets == 1 && queued.status().observedCursor == "1-0"; });
  { std::lock_guard<std::mutex> lock(mutex); release = true; changed.notify_all(); }
  assert(fenced.wait("1-0"));
  assert(fenced.count("1000-0") == 0 && fenced.count("1-0") == 1);

  auto throwing = adapter.subscribeStream("errors", [](const auto&, const auto&, const auto&) {
    throw std::runtime_error("intentional callback failure");
  }, "0-0");
  control.xadd("{" + base + "}:errors", "1-0", fields.begin(), fields.end());
  eventually([&] { return throwing.status().callbackErrors == 1; });

  // Existing read-only credentials can keep delivering when XINFO is denied;
  // inspection unavailability is explicit instead of masquerading as continuity.
  const auto username = "recovery-reader-" + std::to_string(getpid());
  control.command("ACL", "SETUSER", username, "reset", "on", ">test-only", "~{" + base + "}:*",
                  "+ping", "+xread", "+xrevrange");
  {
    auto restrictedOptions = options;
    restrictedOptions.cxn.user = username; restrictedOptions.cxn.password = "test-only";
    RedisAdapter restricted(base, restrictedOptions);
    Observed permitted;
    auto read = restricted.subscribeStream("value", [&](const auto&, const auto&, const auto& entries) {
      permitted.append(entries);
    }, "0-0");
    assert(permitted.wait("5-0"));
    eventually([&] { return read.status().inspectionRejections > 0; });
    assert(!read.status().inspected && read.status().connected);
  }
  control.command<long long>("ACL", "DELUSER", username);
  std::cout << "stream reset, deletion, trim, outage, queue fencing and inspection status passed\n";
}
