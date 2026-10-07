#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <mutex>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using RA = RedisAdapter;

static bool waitUntil(const std::function<bool()>& predicate) {
  const auto end = std::chrono::steady_clock::now() + 4s;
  do { if (predicate()) return true; std::this_thread::sleep_for(5ms); } while (std::chrono::steady_clock::now() < end);
  return predicate();
}
#define EVENTUALLY(...) ASSERT_TRUE(waitUntil([&] { return (__VA_ARGS__); })) << #__VA_ARGS__

struct Seen {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<std::pair<std::string, uint64_t>> entries;
  void append(const RA::StreamBatch& batch, uint64_t epoch = 0) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& entry : batch) entries.emplace_back(entry.first, epoch);
    changed.notify_all();
  }
  bool wait(const std::string& id) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 4s, [&] { for (const auto& entry : entries) if (entry.first == id) return true; return false; });
  }
  size_t count(const std::string& id) { std::lock_guard<std::mutex> lock(mutex); size_t result = 0; for (const auto& entry : entries) result += entry.first == id; return result; }
  size_t size() { std::lock_guard<std::mutex> lock(mutex); return entries.size(); }
  uint64_t epoch(const std::string& id) { std::lock_guard<std::mutex> lock(mutex); for (const auto& entry : entries) if (entry.first == id) return entry.second; return 0; }
};

class Recovery : public testing::TestWithParam<unsigned> {
protected:
  RA_Options options;
  std::string base, user;
  std::unique_ptr<sw::redis::Redis> control;
  RA::Attrs fields{{"_", "value"}};
  void SetUp() override {
    if (!std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) GTEST_SKIP() << "Use the private Redis fixture";
    options.cxn.port = std::stoi(std::getenv("REDIS_ADAPTER_TEST_PORT"));
    options.cxn.timeout = 100;
    options.workers = GetParam();
    base = "recovery-" + std::to_string(getpid()) + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    user = "WRONGTYPE-reader-" + base;
    sw::redis::ConnectionOptions co; co.host = "127.0.0.1"; co.port = options.cxn.port;
    control = std::make_unique<sw::redis::Redis>(co);
    control->command<void>("ACL", "SETUSER", user, "reset", "on", "nopass", "~{" + base + "}:*", "+@all");
    options.cxn.user = user;
  }
  void TearDown() override {
    if (!control) return;
    try { control->command<void>("ACL", "DELUSER", user); } catch (...) {}
    try { std::vector<std::string> keys; control->keys("*" + base + "*", std::back_inserter(keys)); if (!keys.empty()) control->del(keys.begin(), keys.end()); } catch (...) {}
  }
  std::string key(const std::string& sub) const { return "{" + base + "}:" + sub; }
  void add(const std::string& sub, const std::string& id) { EXPECT_EQ(control->xadd(key(sub), id, fields.begin(), fields.end()), id); }
  void recreate(const std::string& sub, const std::string& id) {
    auto transaction = control->transaction();
    transaction.del(key(sub)).xadd(key(sub), id, fields.begin(), fields.end()).exec();
  }
  RA::SubscriptionOptions selection(uint32_t probeMs = 20) {
    RA::SubscriptionOptions result; result.afterId = "0-0"; result.probeMs = probeMs; return result;
  }
  RA::ReaderHandle subscribe(RA& adapter, const std::string& sub, Seen& seen, uint32_t probeMs = 20) {
    return adapter.subscribeStreamWithEpoch(sub, [&](const auto&, const auto&, const auto& batch, uint64_t epoch) { seen.append(batch, epoch); }, selection(probeMs));
  }
};

TEST_P(Recovery, InspectionIsOptInAndCanBeSelectedPerSubscription) {
  EXPECT_EQ(options.readerProbeMs, 0u);
  RA adapter(base, options);
  Seen scalar, imaging;
  auto first = subscribe(adapter, "scalar", scalar, 20);
  auto second = subscribe(adapter, "imaging", imaging, 0);
  EVENTUALLY(first.status().inspected && second.status().connected);
  EXPECT_FALSE(second.status().inspected);
  EXPECT_EQ(first.status().epoch, 1u);
  EXPECT_EQ(RA::ReaderHandle{}.status().epoch, 0u);
}

TEST_P(Recovery, BatchMetadataDoesNotAcknowledgeALaterReadRejection) {
  RA adapter(base, options);
  std::mutex mutex;
  std::condition_variable changed;
  bool entered = false, released = false;
  std::vector<RA::StreamBatchMetadata> observed;
  auto handle = adapter.subscribeStreamWithMetadata("value", [&](const auto&, const auto&, const auto& entries, const auto& metadata) {
    std::unique_lock<std::mutex> lock(mutex);
    if (entries.front().first == "1-0") {
      entered = true; changed.notify_all();
      EXPECT_TRUE(changed.wait_for(lock, 4s, [&] { return released; }));
    }
    observed.push_back(metadata); changed.notify_all();
  }, selection(0));
  add("value", "1-0");
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(changed.wait_for(lock, 3s, [&] { return entered; }));
  }
  control->command<void>("ACL", "SETUSER", user, "-xread");
  EVENTUALLY(handle.status().readRejections > 0);
  {
    std::unique_lock<std::mutex> lock(mutex);
    released = true; changed.notify_all();
    ASSERT_TRUE(changed.wait_for(lock, 3s, [&] { return observed.size() == 1; }));
    EXPECT_EQ(observed[0].readRejections, 0u);
    EXPECT_EQ(observed[0].epoch, 1u);
  }
  EXPECT_GT(handle.status().readRejections, 0u);
  control->command<void>("ACL", "SETUSER", user, "+xread");
  add("value", "2-0");
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(changed.wait_for(lock, 3s, [&] { return observed.size() == 2; }));
    EXPECT_EQ(observed[1].readRejections, handle.status().readRejections);
    EXPECT_EQ(observed[1].epoch, 1u);
  }
}

TEST_P(Recovery, LowerIdRecreationIsDetectedWithZeroAndLongCommandTimeouts) {
  for (const auto timeout : {0u, 3000u}) {
    options.cxn.timeout = timeout;
    const auto sub = "value-" + std::to_string(timeout);
    Seen seen;
    RA adapter(base, options);
    add(sub, "1000-0");
    auto handle = subscribe(adapter, sub, seen);
    ASSERT_TRUE(seen.wait("1000-0"));
    recreate(sub, "1-0");
    ASSERT_TRUE(seen.wait("1-0"));
    EXPECT_EQ(handle.status().streamResets, 1u);
    EXPECT_EQ(handle.status().epoch, 2u);
    EXPECT_EQ(seen.epoch("1-0"), 2u);
  }
}

TEST_P(Recovery, DeletionIsObservedAndLaterDataResumes) {
  Seen seen;
  RA adapter(base, options);
  add("value", "1000-0");
  auto handle = subscribe(adapter, "value", seen);
  ASSERT_TRUE(seen.wait("1000-0"));
  control->del(key("value"));
  EVENTUALLY(handle.status().streamKind == RA::StreamKind::Missing);
  EXPECT_EQ(handle.status().disappearances, 1u);
  EXPECT_FALSE(handle.status().hasData);
  add("value", "1-0");
  ASSERT_TRUE(seen.wait("1-0"));
}

TEST_P(Recovery, RepairAfterXinfoRevocationStillDelivers) {
  Seen seen;
  RA adapter(base, options);
  control->set(key("value"), "wrong type");
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(handle.status().streamKind == RA::StreamKind::Invalid);
  control->command<void>("ACL", "SETUSER", user, "-xinfo");
  recreate("value", "1-0");
  ASSERT_TRUE(seen.wait("1-0"));
  EXPECT_EQ(seen.count("1-0"), 1u);
}

TEST_P(Recovery, StillInvalidAfterXinfoRevocationDoesNotBlockItsNeighbour) {
  Seen bad, healthy;
  RA adapter(base, options);
  control->set(key("bad"), "wrong type");
  auto a = subscribe(adapter, "bad", bad);
  auto b = subscribe(adapter, "healthy", healthy);
  EVENTUALLY(a.status().streamKind == RA::StreamKind::Invalid);
  control->command<void>("ACL", "SETUSER", user, "-xinfo");
  for (unsigned i = 1; i <= 6; ++i) add("healthy", std::to_string(i) + "-0");
  ASSERT_TRUE(healthy.wait("6-0"));
  EXPECT_EQ(healthy.size(), 6u);
  EXPECT_EQ(b.status().readRejections, 0u);
}

TEST_P(Recovery, LegacyRegistrationSurvivesRemovalOfItsOwnedInspectingPeer) {
  Seen legacy;
  RA adapter(base, options);
  control->set(key("value"), "wrong type");
  auto owned = adapter.subscribeStream("value", [](const auto&, const auto&, const auto&) {}, selection());
  ASSERT_TRUE(adapter.addValuesReader<RA::Attrs>("value", [&](const auto&, const auto&, const auto& batch) {
    RA::StreamBatch raw; for (const auto& value : batch) raw.emplace_back(value.first.id(), value.second); legacy.append(raw);
  }));
  EVENTUALLY(owned.status().streamKind == RA::StreamKind::Invalid);
  owned.reset();
  control->del(key("value"));
  // These IDs are valid legacy nanosecond encodings, unlike arbitrary sequence IDs.
  for (unsigned i = 1; i <= 20; ++i) add("value", std::to_string(i) + "-0");
  ASSERT_TRUE(legacy.wait("20-0"));
  EXPECT_EQ(legacy.size(), 20u);
}

TEST_P(Recovery, LegacyWrongTypeIsIsolatedWithoutAnyInspectionPermission) {
  Seen healthy;
  control->command<void>("ACL", "SETUSER", user, "-xinfo");
  RA adapter(base, options);
  control->set(key("legacy"), "wrong type");
  ASSERT_TRUE(adapter.addValuesReader<RA::Attrs>("legacy", [](const auto&, const auto&, const auto&) {}));
  auto owned = subscribe(adapter, "healthy", healthy, 0);
  for (unsigned i = 1; i <= 6; ++i) add("healthy", std::to_string(i) + "-0");
  ASSERT_TRUE(healthy.wait("6-0"));
  EXPECT_EQ(healthy.size(), 6u);
  EXPECT_EQ(owned.status().readRejections, 0u);
}

TEST_P(Recovery, DeniedMetadataAndIdleReadsKeepAccurateConnectionState) {
  Seen seen;
  control->command<void>("ACL", "SETUSER", user, "-xinfo");
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(handle.status().inspectionRejections > 0 && handle.status().connected);
  std::this_thread::sleep_for(300ms);
  const auto status = handle.status();
  EXPECT_FALSE(status.inspected);
  EXPECT_NE(status.streamKind, RA::StreamKind::Invalid); // username contains WRONGTYPE
  EXPECT_EQ(status.socketTimeouts, 0u);
  EXPECT_EQ(status.inspectionRejections, 1u); // denied probes back off
  add("value", "1-0");
  ASSERT_TRUE(seen.wait("1-0"));
}

TEST_P(Recovery, ScopedConnectionKillCountsFailureForEveryRegistrationAndPreservesCursors) {
  Seen first, second;
  RA adapter(base, options);
  auto a = subscribe(adapter, "first", first);
  auto b = subscribe(adapter, "second", second);
  add("first", "10-0"); add("second", "10-0");
  ASSERT_TRUE(first.wait("10-0")); ASSERT_TRUE(second.wait("10-0"));
  const auto failuresA = a.status().readFailures + a.status().inspectionFailures;
  const auto failuresB = b.status().readFailures + b.status().inspectionFailures;
  EXPECT_GT(control->command<long long>("CLIENT", "KILL", "USER", user), 0);
  EXPECT_TRUE(control->ping() == "PONG"); // bystander admin connection survives
  EVENTUALLY(a.status().readFailures + a.status().inspectionFailures > failuresA);
  EVENTUALLY(b.status().readFailures + b.status().inspectionFailures > failuresB);
  EVENTUALLY(a.status().connected && b.status().connected);
  add("first", "11-0"); add("second", "11-0");
  ASSERT_TRUE(first.wait("11-0")); ASSERT_TRUE(second.wait("11-0"));
  EXPECT_EQ(first.count("10-0"), 1u); EXPECT_EQ(second.count("10-0"), 1u);
}

TEST_P(Recovery, EmptyRetainedHistoryCountsACertainGapOnce) {
  Seen seen;
  RA adapter(base, options);
  add("value", "10-0");
  auto handle = subscribe(adapter, "value", seen);
  ASSERT_TRUE(seen.wait("10-0"));
  adapter.setDeferReaders(true);
  add("value", "20-0"); control->xtrim(key("value"), 0, false);
  adapter.setDeferReaders(false);
  EVENTUALLY(handle.status().retentionGaps == 1);
  EXPECT_FALSE(handle.status().hasData);
  EXPECT_EQ(handle.status().cursor, "10-0");
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(handle.status().retentionGaps, 1u);
}

TEST_P(Recovery, QueuedOldEpochIsFencedAndCallbacksReceiveTheirCapturedEpoch) {
  Seen seen;
  RA adapter(base, options);
  std::promise<void> entered, release;
  auto started = entered.get_future(); auto gate = release.get_future().share();
  struct Release { std::promise<void>& promise; ~Release() { try { promise.set_value(); } catch (...) {} } } finally{release};
  const auto blocked = std::string("blocker");
  std::string target;
  for (unsigned i = 0;; ++i) {
    target = "fenced-" + std::to_string(i);
    if (std::hash<std::string>{}(key(target)) % options.workers == std::hash<std::string>{}(key(blocked)) % options.workers) break;
  }
  auto blocker = adapter.subscribeStream(blocked, [&](const auto&, const auto&, const auto&) { entered.set_value(); gate.wait(); }, "0-0");
  add(blocked, "1-0"); ASSERT_EQ(started.wait_for(4s), std::future_status::ready);
  add(target, "1000-0");
  auto handle = subscribe(adapter, target, seen);
  EVENTUALLY(handle.status().observedCursor == "1000-0");
  EXPECT_EQ(handle.status().entries, 0u);
  recreate(target, "1-0");
  EVENTUALLY(handle.status().epoch == 2 && handle.status().observedCursor == "1-0");
  release.set_value();
  ASSERT_TRUE(seen.wait("1-0"));
  EXPECT_EQ(seen.count("1000-0"), 0u);
  EXPECT_EQ(seen.epoch("1-0"), 2u);
  EXPECT_EQ(handle.status().entries, 1u);
}

TEST_P(Recovery, ProbeSchedulingRemainsFairAcrossMoreThan64KeysAndChurn) {
  RA adapter(base, options);
  std::vector<RA::ReaderHandle> handles;
  adapter.setDeferReaders(true);
  for (unsigned i = 0; i < 80; ++i) handles.push_back(adapter.subscribeStream("key-" + std::to_string(i), [](const auto&, const auto&, const auto&) {}, selection()));
  adapter.setDeferReaders(false);
  for (unsigned i = 0; i < 6; ++i) {
    auto churn = adapter.subscribeStream("churn", [](const auto&, const auto&, const auto&) {}, selection());
    std::this_thread::sleep_for(20ms); churn.reset();
  }
  EVENTUALLY(std::all_of(handles.begin(), handles.end(), [](const auto& handle) { return handle.status().inspected; }));
}

TEST_P(Recovery, InactiveHandlesClearHealthAndUnresolvedCursorsAreExplicit) {
  RA::ReaderHandle orphan;
  { RA adapter(base, options); orphan = adapter.subscribeStream("key", [](const auto&, const auto&, const auto&) {}, selection()); EVENTUALLY(orphan.status().connected); }
  EXPECT_FALSE(orphan.status().active);
  EXPECT_FALSE(orphan.status().connected);
  EXPECT_FALSE(orphan.status().inspected);
  orphan.reset(); EXPECT_EQ(orphan.status().epoch, 0u);
  auto unavailable = options; unavailable.cxn.port = 0;
  RA adapter(base, unavailable);
  auto pending = adapter.subscribeStream("pending", [](const auto&, const auto&, const auto&) {});
  EXPECT_TRUE(pending.status().cursor.empty());
  EXPECT_TRUE(pending.status().observedCursor.empty());
}

TEST_P(Recovery, CallbackErrorsAreCountedWithoutStoppingDelivery) {
  Seen seen;
  RA adapter(base, options);
  auto bad = adapter.subscribeStream("key", [](const auto&, const auto&, const auto&) { throw std::runtime_error("intentional"); }, selection());
  auto good = subscribe(adapter, "key", seen);
  add("key", "1-0"); ASSERT_TRUE(seen.wait("1-0"));
  EVENTUALLY(bad.status().callbackErrors == 1);
}

TEST_P(Recovery, RetainedHistoryAboveAnExplicitCursorReportsAPossibleGap) {
  Seen seen;
  RA adapter(base, options);
  add("trimmed", "10-0"); add("trimmed", "20-0");
  control->xtrim(key("trimmed"), 1, false);
  auto selected = selection(); selected.afterId = "10-0";
  auto handle = adapter.subscribeStreamWithEpoch("trimmed", [&](const auto&, const auto&, const auto& entries, uint64_t epoch) { seen.append(entries, epoch); }, selected);
  ASSERT_TRUE(seen.wait("20-0"));
  EXPECT_EQ(handle.status().retentionGaps, 1u);
  EXPECT_EQ(handle.status().streamResets, 0u);
}

TEST_P(Recovery, AuthenticationFailureClearsConnectedEvenWithoutInspection) {
  Seen seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen, 0);
  EVENTUALLY(handle.status().connected);
  control->command<void>("ACL", "SETUSER", user, "resetpass", ">new-test-password");
  EXPECT_GT(control->command<long long>("CLIENT", "KILL", "USER", user), 0);
  EVENTUALLY(handle.status().readFailures > 0 && !handle.status().connected);
  EXPECT_FALSE(handle.status().inspected);
}

INSTANTIATE_TEST_SUITE_P(Workers, Recovery, testing::Values(1u, 4u));
