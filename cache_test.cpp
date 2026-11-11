#include "RedisCache.hpp"
#include <gtest/gtest.h>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <unistd.h>

using namespace std::chrono_literals;

class LatestCache : public testing::Test {
protected:
  std::shared_ptr<RedisAdapter> adapter;
  std::unique_ptr<RedisCache<int32_t>> cache;
  std::unique_ptr<sw::redis::Redis> control;
  RedisAdapter::ReaderHandle observer;
  std::string base, key, lastSeen;
  std::mutex mutex;
  std::condition_variable changed;
  size_t largestBatch = 0;

  static RedisAdapter::Attrs fields(const std::vector<int32_t>& values) {
    return {{"_", std::string(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(int32_t))}};
  }

  void SetUp() override {
    const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
    if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST"))
      GTEST_SKIP() << "Use the private Redis test fixture";
    RA_Options options;
    options.cxn.port = std::stoi(port);
    options.cxn.timeout = 100;
    options.workers = 1;
    base = "cache-" + std::to_string(getpid()) + "-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    key = "{" + base + "}:value";
    sw::redis::ConnectionOptions connection;
    connection.host = "127.0.0.1";
    connection.port = options.cxn.port;
    control = std::make_unique<sw::redis::Redis>(connection);
    const auto initial = fields({-1});
    control->xadd(key, "1-0", initial.begin(), initial.end());
    adapter = std::make_shared<RedisAdapter>(base, options);
    cache = std::make_unique<RedisCache<int32_t>>(adapter, "value");
    // This callback shares the reader's ordered worker with the cache callback.
    // Seeing a batch here proves that its preceding cache update completed.
    observer = adapter->subscribeStream("value", [this](const auto&, const auto&, const auto& batch) {
      std::lock_guard<std::mutex> lock(mutex);
      largestBatch = std::max(largestBatch, batch.size());
      lastSeen = batch.back().first;
      changed.notify_all();
    }, "1-0");
  }

  void TearDown() override {
    observer.reset();
    cache.reset();
    adapter.reset();
    if (control) control->del(key);
  }

  bool waitFor(const std::string& id) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 3s, [&] { return lastSeen == id; });
  }

  void expectValue(const std::vector<int32_t>& expected, const std::string& id) {
    std::vector<int32_t> actual;
    const auto time = cache->copyReadBuffer(actual);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(time.value, RA_Time(id).value);
  }
};

TEST_F(LatestCache, KeepsNewestValueAndTimestampFromOneBatch) {
  auto transaction = control->transaction();
  for (int i = 2; i <= 4; ++i) {
    const auto data = fields({i, i * 10});
    transaction.xadd(key, std::to_string(i) + "-0", data.begin(), data.end());
  }
  // EXEC makes the complete backlog available before the blocked reader wakes.
  transaction.exec();
  ASSERT_TRUE(waitFor("4-0"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(largestBatch, 3u);
  }
  expectValue({4, 40}, "4-0");
}

TEST_F(LatestCache, KeepsSingleEntryAndIgnoresFullyRejectedBatch) {
  const auto data = fields({42});
  control->xadd(key, "2-0", data.begin(), data.end());
  ASSERT_TRUE(waitFor("2-0"));
  expectValue({42}, "2-0");
  const RedisAdapter::Attrs malformed{{"_", "x"}};
  control->xadd(key, "3-0", malformed.begin(), malformed.end());
  ASSERT_TRUE(waitFor("3-0"));
  expectValue({42}, "2-0");
}

TEST_F(LatestCache, AcceptsAnEmptyArrayAsTheLatestValue) {
  const RedisAdapter::Attrs empty{{"_", ""}};
  control->xadd(key, "2-0", empty.begin(), empty.end());
  ASSERT_TRUE(waitFor("2-0"));
  expectValue({}, "2-0");
}

static bool waitForUpdate(RedisCache<int32_t>& cache) {
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (!cache.newValueAvaliable() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(1ms);
  return cache.newValueAvaliable();
}

TEST_F(LatestCache, DestructionKeepsSharedObserversAndReplacementCachesAlive) {
  cache.reset();
  auto data = fields({2});
  control->xadd(key, "2-0", data.begin(), data.end());
  ASSERT_TRUE(waitFor("2-0"));

  cache = std::make_unique<RedisCache<int32_t>>(adapter, "value");
  data = fields({3});
  control->xadd(key, "3-0", data.begin(), data.end());
  ASSERT_TRUE(waitForUpdate(*cache));
  expectValue({3}, "3-0");
  ASSERT_TRUE(waitFor("3-0"));
}

TEST_F(LatestCache, IndependentCachesRetireOnlyTheirOwnRegistration) {
  auto peer = std::make_unique<RedisCache<int32_t>>(adapter, "value");
  cache.reset();
  auto data = fields({2});
  control->xadd(key, "2-0", data.begin(), data.end());
  ASSERT_TRUE(waitForUpdate(*peer));
  std::vector<int32_t> actual;
  EXPECT_EQ(peer->copyReadBuffer(actual).value, RA_Time("2-0").value);
  EXPECT_EQ(actual, (std::vector<int32_t>{2}));

  // Match a duplicate map insertion that constructs and discards a cache.
  { auto duplicate = std::make_unique<RedisCache<int32_t>>(adapter, "value"); }
  peer->clearNewValueAvaliable();
  data = fields({3});
  control->xadd(key, "3-0", data.begin(), data.end());
  ASSERT_TRUE(waitForUpdate(*peer));
  EXPECT_EQ(peer->copyReadBuffer(actual).value, RA_Time("3-0").value);
  EXPECT_EQ(actual, (std::vector<int32_t>{3}));
  peer.reset();
  data = fields({4});
  control->xadd(key, "4-0", data.begin(), data.end());
  ASSERT_TRUE(waitFor("4-0"));
}

TEST_F(LatestCache, SiblingCallbackCanDestroyACacheWithQueuedDelivery) {
  cache.reset();
  auto slot = std::make_shared<std::unique_ptr<RedisCache<int32_t>>>();
  auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto retiring = adapter->subscribeStream("value", [slot, destroyed](const auto&, const auto&, const auto&) {
    slot->reset();
    *destroyed = true;
  }, "1-0");
  *slot = std::make_unique<RedisCache<int32_t>>(adapter, "value");
  const auto data = fields({2});
  control->xadd(key, "2-0", data.begin(), data.end());
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (!destroyed->load() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(destroyed->load());
  retiring.reset();
  control->xadd(key, "3-0", data.begin(), data.end());
  ASSERT_TRUE(waitFor("3-0"));
}

TEST_F(LatestCache, DeferredCachesKeepTheLegacyStartAtTailBehavior) {
  adapter->setDeferReaders(true);
  cache.reset();
  cache = std::make_unique<RedisCache<int32_t>>(adapter, "value");
  auto data = fields({2});
  control->xadd(key, "2-0", data.begin(), data.end());
  adapter->setDeferReaders(false);
  ASSERT_TRUE(waitFor("2-0"));
  EXPECT_FALSE(cache->newValueAvaliable());
  expectValue({2}, "2-0");
  data = fields({3});
  control->xadd(key, "3-0", data.begin(), data.end());
  ASSERT_TRUE(waitForUpdate(*cache));
  expectValue({3}, "3-0");
}

TEST_F(LatestCache, CacheCanReleaseTheLastAdapterOwner) {
  std::weak_ptr<RedisAdapter> owner = adapter;
  adapter.reset();
  EXPECT_FALSE(owner.expired());
  cache.reset();
  EXPECT_TRUE(owner.expired());
  EXPECT_FALSE(observer);
}
