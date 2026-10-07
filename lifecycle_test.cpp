#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <sstream>
#include <unistd.h>

using namespace std::chrono_literals;
using RA = RedisAdapter;

struct Observations {
  std::mutex mutex;
  std::condition_variable changed;
  RA::StreamBatch entries;
  size_t largest = 0;
  void append(const RA::StreamBatch& batch) {
    std::lock_guard<std::mutex> guard(mutex);
    largest = std::max(largest, batch.size());
    entries.insert(entries.end(), batch.begin(), batch.end());
    changed.notify_all();
  }
  bool waitFor(const std::string& id) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 3s, [&] {
      for (const auto& entry : entries) if (entry.first == id) return true;
      return false;
    });
  }
  size_t size() { std::lock_guard<std::mutex> guard(mutex); return entries.size(); }
};

class Lifecycle : public testing::TestWithParam<unsigned> {
protected:
  RA_Options options;
  std::string base;
  std::unique_ptr<sw::redis::Redis> control;
  void SetUp() override {
    const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
    if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) GTEST_SKIP() << "Use the private Redis test fixture";
    options.cxn.port = std::stoi(port);
    options.cxn.timeout = 100;
    options.workers = GetParam();
    base = "lifecycle-" + std::to_string(getpid()) + "-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    sw::redis::ConnectionOptions connection;
    connection.host = "127.0.0.1";
    connection.port = options.cxn.port;
    control = std::make_unique<sw::redis::Redis>(connection);
  }
  void TearDown() override {
    if (!control) return;
    try {
      std::vector<std::string> keys;
      control->keys("*" + base + "*", std::back_inserter(keys));
      if (!keys.empty()) control->del(keys.begin(), keys.end());
    } catch (...) {}
  }
  std::string key(const std::string& sub) const { return "{" + base + "}:" + sub; }
  std::string add(const std::string& sub, const std::string& id) {
    RA::Attrs fields{{"_", "raw"}};
    return control->xadd(key(sub), id, fields.begin(), fields.end());
  }
};

TEST(StreamDecoding, RejectsMalformedRepresentationsAndKeepsDestination) {
  double scalar = 9.;
  EXPECT_FALSE(RA::decodeScalar<double>({{"_", "x"}}, scalar));
  EXPECT_EQ(scalar, 9.);
  EXPECT_FALSE(RA::decodeScalar<double>({{"_", std::string(16, '\0')}}, scalar));
  std::vector<double> array{9.};
  EXPECT_FALSE(RA::decodeArray<double>({{"_", "x"}}, array));
  EXPECT_EQ(array, std::vector<double>{9.});
  EXPECT_TRUE(RA::decodeArray<double>({{"_", ""}}, array));
  EXPECT_TRUE(array.empty());
  EXPECT_FALSE(RA::decodeArray<double>({}, array));
  EXPECT_FALSE(RA::decodeArray<double>({{"_", std::string(16, '\0')}}, array, 8));
  bool flag = true;
  EXPECT_FALSE(RA::decodeScalar<bool>({{"_", std::string(sizeof(bool), '\xff')}}, flag));
  EXPECT_TRUE(flag);
  const bool native[] = {true, false, true};
  const std::string bytes(reinterpret_cast<const char*>(native), sizeof(native));
  std::vector<bool> flags;
  ASSERT_TRUE(RA::decodeArray<bool>({{"in", bytes}}, flags, bytes.size(), "in"));
  EXPECT_EQ(flags, (std::vector<bool>{true, false, true}));
  auto invalid = bytes; invalid[sizeof(bool)] = '\xff';
  EXPECT_FALSE(RA::decodeArray<bool>({{"_", invalid}}, flags));
  EXPECT_EQ(flags, (std::vector<bool>{true, false, true}));
}

TEST(StreamDecoding, PreservesOpaqueOrderingAndValidatesIds) {
  EXPECT_LT(RA::compareStreamIds("1-18446744073709551615", "2-0"), 0);
  EXPECT_LT(RA::compareStreamIds("123-999999999999", "123-1000000000000"), 0);
  EXPECT_EQ(RA::compareStreamIds("5", "5-0"), 0);
  EXPECT_THROW(RA::compareStreamIds("1-2x", "1-2"), std::invalid_argument);
  EXPECT_THROW(RA::compareStreamIds("", "0-0"), std::invalid_argument);
  EXPECT_EQ(RA_Time("18446744073709551615-1").value, 0);
  EXPECT_EQ(RA_Time("12x-1").value, 0);
}

TEST_P(Lifecycle, SnapshotClosesTheRegistrationGapAndPreservesExactIds) {
  Observations seen;
  RA adapter(base, options);
  ASSERT_EQ(add("key", "1-18446744073709551615"), "1-18446744073709551615");
  const auto snapshot = adapter.getStreamSnapshot("key");
  ASSERT_TRUE(snapshot.connected);
  ASSERT_EQ(snapshot.id, "1-18446744073709551615");
  ASSERT_EQ(add("key", "2-0"), "2-0");
  auto handle = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, snapshot.id);
  EXPECT_TRUE(seen.waitFor("2-0"));
  EXPECT_EQ(seen.size(), 1u);
}

TEST_P(Lifecycle, DefaultTailAndSharedRewindFilterDuplicates) {
  Observations first, second;
  RA adapter(base, options);
  ASSERT_EQ(add("shared", "1-0"), "1-0");
  auto a = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& batch) { first.append(batch); });
  ASSERT_EQ(add("shared", "2-0"), "2-0");
  ASSERT_TRUE(first.waitFor("2-0"));
  EXPECT_EQ(first.size(), 1u);
  auto b = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& batch) { second.append(batch); }, "0-0");
  ASSERT_TRUE(second.waitFor("2-0"));
  ASSERT_EQ(add("shared", "3-0"), "3-0");
  ASSERT_TRUE(first.waitFor("3-0"));
  ASSERT_TRUE(second.waitFor("3-0"));
  EXPECT_EQ(first.size(), 2u);
  EXPECT_EQ(second.size(), 3u);
  a.reset();
  ASSERT_EQ(add("shared", "4-0"), "4-0");
  ASSERT_TRUE(second.waitFor("4-0"));
  EXPECT_EQ(first.size(), 2u);
}

TEST_P(Lifecycle, OwnedDeferralRetainsUpdatesAndLegacyDeferralStartsAtTail) {
  Observations owned;
  std::atomic<unsigned> legacy{0};
  RA adapter(base, options);
  ASSERT_TRUE(adapter.setDeferReaders(true));
  ASSERT_EQ(add("owned", "1-0"), "1-0");
  auto handle = adapter.subscribeStream("owned", [&](const auto&, const auto&, const auto& batch) { owned.append(batch); });
  adapter.addValuesReader<RA::Attrs>("legacy", [&](const auto&, const auto&, const auto&) { ++legacy; });
  ASSERT_EQ(add("owned", "2-0"), "2-0");
  ASSERT_EQ(add("legacy", "1-0"), "1-0");
  ASSERT_TRUE(adapter.setDeferReaders(false));
  ASSERT_TRUE(owned.waitFor("2-0"));
  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(owned.size(), 1u);
  EXPECT_EQ(legacy.load(), 0u);
}

TEST_P(Lifecycle, LegacyRemovalReleasesSiblingHandlesWithoutHoldingReaderLock) {
  RA adapter(base, options);
  auto sibling = std::make_shared<RA::ReaderHandle>(adapter.subscribeStream("sibling", [](const auto&, const auto&, const auto&) {}, "0-0"));
  ASSERT_TRUE(adapter.addValuesReader<RA::Attrs>("legacy", [sibling](const auto&, const auto&, const auto&) {}));
  sibling.reset();
  EXPECT_TRUE(adapter.removeReader("legacy"));
  auto genericSibling = std::make_shared<RA::ReaderHandle>(adapter.subscribeStream("sibling", [](const auto&, const auto&, const auto&) {}, "0-0"));
  ASSERT_TRUE(adapter.addGenericReader(base + "-generic", [genericSibling](const auto&, const auto&, const auto&) {}));
  genericSibling.reset();
  EXPECT_TRUE(adapter.removeGenericReader(base + "-generic"));
}

TEST_P(Lifecycle, RemoveReaderInvalidatesAllHandlesButHandleResetKeepsPeers) {
  Observations seen;
  RA adapter(base, options);
  auto a = adapter.subscribeStream("key", [](const auto&, const auto&, const auto&) {}, "0-0");
  auto b = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, "0-0");
  a.reset();
  ASSERT_EQ(add("key", "1-0"), "1-0");
  ASSERT_TRUE(seen.waitFor("1-0"));
  EXPECT_TRUE(adapter.removeReader("key"));
  EXPECT_FALSE(b);
}

TEST_P(Lifecycle, CallbackExceptionsAndLastOwnerReleaseDoNotStopThePool) {
  Observations seen;
  RA adapter(base, options);
  auto throwing = adapter.subscribeStream("key", [](const auto&, const auto&, const auto&) { throw std::runtime_error("intentional"); }, "0-0");
  auto healthy = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, "0-0");
  ASSERT_EQ(add("key", "1-0"), "1-0");
  ASSERT_TRUE(seen.waitFor("1-0"));
  std::promise<void> destroyed;
  auto ready = destroyed.get_future();
  auto last = std::make_shared<RA>(base, options);
  auto handle = last->subscribeStream("last", [&](const auto&, const auto&, const auto&) { last.reset(); destroyed.set_value(); }, "0-0");
  ASSERT_EQ(add("last", "1-0"), "1-0");
  ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
  handle.reset();
  RA::ReaderHandle orphan;
  { RA temporary(base, options); orphan = temporary.subscribeStream("orphan", [](const auto&, const auto&, const auto&) {}, "0-0"); }
  EXPECT_FALSE(orphan);
  orphan.reset();
}

TEST_P(Lifecycle, BatchCountBoundsBacklogAndMalformedTypedBatchesAreNotDelivered) {
  Observations seen;
  options.readerBatchCount = 7;
  RA adapter(base, options);
  for (unsigned i = 1; i <= 80; ++i) ASSERT_EQ(add("backlog", std::to_string(i) + "-0"), std::to_string(i) + "-0");
  auto handle = adapter.subscribeStream("backlog", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, "0-0");
  ASSERT_TRUE(seen.waitFor("80-0"));
  EXPECT_EQ(seen.size(), 80u);
  EXPECT_LE(seen.largest, 7u);
  std::atomic<unsigned> calls{0};
  ASSERT_TRUE(adapter.addListsReader<double>("malformed", [&](const auto&, const auto&, const auto&) { ++calls; }));
  ASSERT_EQ(add("malformed", "1-0"), "1-0");
  std::this_thread::sleep_for(250ms);
  EXPECT_EQ(calls.load(), 0u);
  RA producer(base, options);
  const auto valid = producer.addSingleList<double>("malformed", std::vector<double>{}, {.trim=0});
  ASSERT_TRUE(valid.ok());
  const auto end = std::chrono::steady_clock::now() + 3s;
  while (!calls && std::chrono::steady_clock::now() < end) std::this_thread::sleep_for(5ms);
  EXPECT_EQ(calls.load(), 1u);
}

TEST_P(Lifecycle, SnapshotRejectionIsNotADisconnectOrReplayCursor) {
  RA adapter(base, options);
  control->set(key("wrong"), "wrong type");
  ASSERT_TRUE(adapter.connected());
  const auto before = control->command<std::string>("CLIENT", "LIST");
  for (int i = 0; i < 20; ++i) {
    const auto snapshot = adapter.getStreamSnapshot("wrong");
    EXPECT_FALSE(snapshot.connected);
    EXPECT_TRUE(snapshot.rejected);
    EXPECT_EQ(snapshot.id, "$");
  }
  const auto after = control->command<std::string>("CLIENT", "LIST");
  EXPECT_EQ(std::count(before.begin(), before.end(), '\n'), std::count(after.begin(), after.end(), '\n'));
  const auto empty = adapter.getStreamSnapshot("missing");
  EXPECT_TRUE(empty.connected);
  EXPECT_EQ(empty.id, "0-0");
}

TEST_P(Lifecycle, ZeroTimeoutAndBraceKeysStillCancelWithinFiniteCycles) {
  options.cxn.timeout = 0;
  RA adapter(base, options);
  const auto start = std::chrono::steady_clock::now();
  ASSERT_TRUE(adapter.addGenericReader(base + "{", [](const auto&, const auto&, const auto&) {}));
  EXPECT_TRUE(adapter.removeGenericReader(base + "{"));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
}

TEST_P(Lifecycle, InitiallyUnavailableReadersRecoverWithoutAdapterOperations) {
  const auto* socket = std::getenv("REDIS_ADAPTER_TEST_SOCKET");
  ASSERT_NE(socket, nullptr);
  const auto path = std::filesystem::temp_directory_path() / (base + ".sock");
  struct Cleanup { std::filesystem::path path; ~Cleanup() { std::error_code ec; std::filesystem::remove(path, ec); } } cleanup{path};
  options.cxn.path = path.string();
  options.cxn.connectTimeout = 50;
  Observations seen, legacy, generic;
  RA adapter(base, options);
  auto handle = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, "0-0");
  ASSERT_TRUE(handle);
  ASSERT_TRUE(adapter.addValuesReader<RA::Attrs>("legacy", [&](const auto& callbackBase, const auto& sub, const auto& batch) {
    EXPECT_EQ(callbackBase, base);
    EXPECT_EQ(sub, "legacy");
    RA::StreamBatch raw;
    for (const auto& entry : batch) raw.emplace_back(entry.first.id(), entry.second);
    legacy.append(raw);
  }));
  const auto genericKey = base + "-generic";
  ASSERT_TRUE(adapter.addGenericReader(genericKey, [&](const auto& callbackBase, const auto& sub, const auto& batch) {
    EXPECT_EQ(callbackBase, genericKey);
    EXPECT_EQ(sub, genericKey);
    RA::StreamBatch raw;
    for (const auto& entry : batch) raw.emplace_back(entry.first.id(), entry.second);
    generic.append(raw);
  }));
  std::filesystem::create_symlink(socket, path);
  // Only the independent control connection writes after socket recovery.
  // Delivery proves that the reader loop recovered the initially absent client.
  ASSERT_EQ(add("key", "1-0"), "1-0");
  ASSERT_TRUE(seen.waitFor("1-0"));
  // All readers share the default bucket, so their '$' tails have resolved
  // before the owned callback is delivered. Subsequent entries must flow.
  ASSERT_EQ(add("legacy", "2-0"), "2-0");
  const RA::Attrs fields{{"_", "raw"}};
  ASSERT_EQ(control->xadd(genericKey, "3-0", fields.begin(), fields.end()), "3-0");
  EXPECT_TRUE(legacy.waitFor("2-0"));
  EXPECT_TRUE(generic.waitFor("3-0"));
  EXPECT_EQ(seen.size(), 1u);
  EXPECT_EQ(legacy.size(), 1u);
  EXPECT_EQ(generic.size(), 1u);
  {
    std::lock_guard<std::mutex> lock(legacy.mutex);
    if (!legacy.entries.empty()) EXPECT_EQ(legacy.entries.front().second, fields);
  }
  {
    std::lock_guard<std::mutex> lock(generic.mutex);
    if (!generic.entries.empty()) EXPECT_EQ(generic.entries.front().second, fields);
  }
}

TEST_P(Lifecycle, ReadOnlyAclCanResolveDefaultTailUsingXreadOnRedis74) {
  const auto info = control->command<std::string>("INFO", "server");
  if (info.find("redis_version:7.0.") != std::string::npos || info.find("redis_version:7.2.") != std::string::npos)
    GTEST_SKIP() << "XREAD '+' requires the supported Redis 7.4 target";
  const auto user = base + "-user";
  struct Cleanup { sw::redis::Redis& redis; std::string user; ~Cleanup() { try { redis.command<void>("ACL", "DELUSER", user); } catch (...) {} } } cleanup{*control, user};
  control->command<void>("ACL", "SETUSER", user, "on", "nopass", "~*", "+ping", "+xread", "+xadd");
  options.cxn.user = user;
  Observations seen;
  RA adapter(base, options);
  ASSERT_EQ(add("key", "1-0"), "1-0");
  auto handle = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); });
  ASSERT_EQ(add("key", "2-0"), "2-0");
  EXPECT_TRUE(seen.waitFor("2-0"));
  EXPECT_EQ(seen.size(), 1u);
}

TEST_P(Lifecycle, ResetFencesCallbacksStillQueuedBehindASibling) {
  RA adapter(base, options);
  std::promise<void> entered, release;
  auto enteredFuture = entered.get_future();
  auto gate = release.get_future().share();
  struct Release { std::promise<void>& release; ~Release() { try { release.set_value(); } catch (...) {} } } finally{release};
  std::atomic<unsigned> queuedCalls{0};
  auto blocker = adapter.subscribeStream("queued", [&](const auto&, const auto&, const auto&) { entered.set_value(); gate.wait(); }, "0-0");
  auto queued = adapter.subscribeStream("queued", [&](const auto&, const auto&, const auto&) { ++queuedCalls; }, "0-0");
  ASSERT_EQ(add("queued", "1-0"), "1-0");
  ASSERT_EQ(enteredFuture.wait_for(3s), std::future_status::ready);
  queued.reset();
  release.set_value();
  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(queuedCalls.load(), 0u);
}

TEST_P(Lifecycle, CompleteFreshBatchesShareTheirStorageAcrossRegistrations) {
  Observations first, second;
  std::atomic<const void*> firstBatch{nullptr}, secondBatch{nullptr};
  RA adapter(base, options);
  auto a = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& batch) { firstBatch = &batch; first.append(batch); }, "0-0");
  auto b = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& batch) { secondBatch = &batch; second.append(batch); }, "0-0");
  ASSERT_EQ(add("shared", "1-0"), "1-0");
  ASSERT_TRUE(first.waitFor("1-0"));
  ASSERT_TRUE(second.waitFor("1-0"));
  EXPECT_NE(firstBatch.load(), nullptr);
  EXPECT_EQ(firstBatch.load(), secondBatch.load());
}

TEST_P(Lifecycle, WrongTypeDefaultCursorKeepsEveryEntryAfterRepair) {
  Observations fast, slow;
  RA adapter(base, options);
  control->set(key("slow"), "not a stream");
  auto a = adapter.subscribeStream("fast", [&](const auto&, const auto&, const auto& batch) { fast.append(batch); });
  auto b = adapter.subscribeStream("slow", [&](const auto&, const auto&, const auto& batch) { slow.append(batch); });
  control->del(key("slow"));
  for (unsigned i = 1; i <= 50; ++i) {
    const auto id = std::to_string(i) + "-0";
    ASSERT_EQ(add("fast", id), id);
    ASSERT_EQ(add("slow", id), id);
  }
  ASSERT_TRUE(fast.waitFor("50-0"));
  ASSERT_TRUE(slow.waitFor("50-0"));
  EXPECT_EQ(fast.size(), 50u);
  EXPECT_EQ(slow.size(), 50u);
}

TEST_P(Lifecycle, MalformedSingleItemsHaveADistinctStatusAndPreserveDestination) {
  RA adapter(base, options);
  ASSERT_EQ(add("bad", "1-0"), "1-0");
  double scalar = 42.;
  EXPECT_EQ(adapter.getSingleValue("bad", scalar), RA_INVALID_PAYLOAD);
  EXPECT_EQ(scalar, 42.);
  std::vector<double> list{42.};
  EXPECT_EQ(adapter.getSingleList("bad", list), RA_INVALID_PAYLOAD);
  EXPECT_EQ(list, (std::vector<double>{42.}));
  EXPECT_EQ(adapter.getSingleList("absent", list).value, 0);
}

TEST_P(Lifecycle, ListenOnlyReaderRetriesAfterItsOwnConnectionIsClosed) {
  Observations seen;
  RA adapter(base, options);
  auto handle = adapter.subscribeStream("key", [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, "0-0");
  ASSERT_EQ(add("key", "1-0"), "1-0");
  ASSERT_TRUE(seen.waitFor("1-0"));
  std::string readerId;
  const auto end = std::chrono::steady_clock::now() + 2s;
  do {
    const auto clients = control->command<std::string>("CLIENT", "LIST");
    std::istringstream lines(clients);
    for (std::string line; std::getline(lines, line);) {
      // The fixture is private and this test is serial. The only XREAD client
      // belongs to this adapter; kill that returned ID rather than all clients.
      if (line.find("cmd=xread") != std::string::npos) {
        const auto begin = line.find("id=") + 3;
        readerId = line.substr(begin, line.find(' ', begin) - begin);
      }
    }
    if (readerId.empty()) std::this_thread::sleep_for(5ms);
  } while (readerId.empty() && std::chrono::steady_clock::now() < end);
  ASSERT_FALSE(readerId.empty());
  EXPECT_EQ(control->command<long long>("CLIENT", "KILL", "ID", readerId), 1);
  ASSERT_EQ(add("key", "2-0"), "2-0");
  ASSERT_TRUE(seen.waitFor("2-0"));
  EXPECT_EQ(seen.size(), 2u);
}

TEST_P(Lifecycle, RunningCaptureCleanupCanRetireASiblingRegistration) {
  for (const bool throwing : {false, true}) {
    RA adapter(base, options);
    std::promise<void> entered, release, retired;
    auto enteredFuture = entered.get_future();
    auto retiredFuture = retired.get_future();
    auto gate = release.get_future().share();
    struct Release { std::promise<void>& release; ~Release() { try { release.set_value(); } catch (...) {} } } finally{release};
    struct Captured {
      RA::ReaderHandle sibling;
      std::promise<void>& retired;
      ~Captured() { sibling.reset(); retired.set_value(); }
    };
    const auto suffix = throwing ? "throw" : "plain";
    const auto currentKey = std::string("current-") + suffix;
    std::string siblingKey;
    for (unsigned i = 0;; ++i) {
      siblingKey = "sibling-" + std::to_string(i) + "-" + suffix;
      if (std::hash<std::string>{}(key(siblingKey)) % options.workers ==
          std::hash<std::string>{}(key(currentKey)) % options.workers) break;
    }
    auto state = std::shared_ptr<Captured>(new Captured{
        adapter.subscribeStream(siblingKey, [](const auto&, const auto&, const auto&) {}, "0-0"), retired});
    auto current = adapter.subscribeStream(currentKey, [state, &entered, gate, throwing](const auto&, const auto&, const auto&) {
      entered.set_value(); gate.wait();
      if (throwing) throw std::runtime_error("intentional captured-state exception");
    }, "0-0");
    ASSERT_EQ(add(currentKey, "1-0"), "1-0");
    ASSERT_EQ(enteredFuture.wait_for(3s), std::future_status::ready);
    current.reset();
    state.reset();
    for (unsigned i = 1; i <= 200; ++i) ASSERT_EQ(add(siblingKey, std::to_string(i) + "-0"), std::to_string(i) + "-0");
    release.set_value();
    EXPECT_EQ(retiredFuture.wait_for(3s), std::future_status::ready);
  }
}

TEST_P(Lifecycle, LegacyRemovalCannotRestartAReaderDuringDestruction) {
  std::promise<void> entered, release, callbackDone;
  auto enteredFuture = entered.get_future();
  auto callbackFuture = callbackDone.get_future();
  auto gate = release.get_future().share();
  std::atomic<bool> removed{false};
  auto adapter = std::make_unique<RA>(base, options);
  struct ReleaseEarly { std::promise<void>& release; ~ReleaseEarly() { try { release.set_value(); } catch (...) {} } } releaseOnFailure{release};
  auto* raw = adapter.get();
  auto quiet = adapter->subscribeStream("quiet", [](const auto&, const auto&, const auto&) {}, "0-0");
  auto blocked = adapter->subscribeStream("blocked", [&](const auto&, const auto&, const auto&) {
    entered.set_value(); gate.wait();
    removed = raw->removeReader("quiet");
    callbackDone.set_value();
  }, "0-0");
  ASSERT_EQ(add("blocked", "1-0"), "1-0");
  ASSERT_EQ(enteredFuture.wait_for(3s), std::future_status::ready);
  std::thread destroying([owned = std::move(adapter)]() mutable { owned.reset(); });
  struct Finish {
    std::promise<void>& release; std::thread& thread;
    ~Finish() { try { release.set_value(); } catch (...) {} if (thread.joinable()) thread.join(); }
  } finally{release, destroying};
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (blocked && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
  ASSERT_FALSE(blocked); // destructor has stopped readers and fenced registrations
  release.set_value();
  ASSERT_EQ(callbackFuture.wait_for(3s), std::future_status::ready);
  EXPECT_TRUE(removed.load());
}

INSTANTIATE_TEST_SUITE_P(Workers, Lifecycle, testing::Values(1u, 4u));
