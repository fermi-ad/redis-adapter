#include "RedisAdapter.hpp"
#include "RedisLargeBufferCache.hpp"
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <vector>

using redis_adapter_detail::LargeBufferCache;
using RA = RedisAdapter;

TEST(LargeBufferCache, SmallRequestsBypassTheCache) {
  LargeBufferCache cache;
  void* small = cache.allocate(100);
  ASSERT_NE(small, nullptr);
  EXPECT_EQ(cache.liveBlocks(), 0u);
  cache.release(small);
  EXPECT_EQ(cache.cachedBlocks(), 0u);
}

TEST(LargeBufferCache, FreedBlocksAreReused) {
  LargeBufferCache cache;
  void* first = cache.allocate(300 * 1024);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(cache.liveBlocks(), 1u);
  cache.release(first);
  EXPECT_EQ(cache.cachedBlocks(), 1u);
  EXPECT_EQ(cache.cachedBytes(), LargeBufferCache::capacityFor(300 * 1024));
  // Any request that fits gets the cached block back instead of a new one.
  void* second = cache.allocate(200 * 1024);
  EXPECT_EQ(second, first);
  EXPECT_EQ(cache.cachedBlocks(), 0u);
  cache.release(second);
}

TEST(LargeBufferCache, ReallocWithinCapacityKeepsTheBlock) {
  LargeBufferCache cache;
  auto* block = static_cast<char*>(cache.allocate(70 * 1024));   // capacity 128 KiB
  ASSERT_NE(block, nullptr);
  std::memset(block, 'x', 70 * 1024);
  EXPECT_EQ(cache.reallocate(block, 120 * 1024), block);
  auto* grown = static_cast<char*>(cache.reallocate(block, 600 * 1024));
  ASSERT_NE(grown, nullptr);
  EXPECT_NE(grown, block);
  for (size_t i = 0; i != 70 * 1024; ++i) ASSERT_EQ(grown[i], 'x');
  EXPECT_EQ(cache.liveBlocks(), 1u);
  EXPECT_EQ(cache.cachedBlocks(), 1u);                            // the old block was kept
  cache.release(grown);
}

TEST(LargeBufferCache, PointersFromMallocAreHandled) {
  LargeBufferCache cache;
  // Allocated before the cache saw them, as hiredis buffers created before installation are.
  auto* small = static_cast<char*>(std::malloc(1000));
  std::memset(small, 'a', 1000);
  auto* grown = static_cast<char*>(cache.reallocate(small, 100 * 1024));
  ASSERT_NE(grown, nullptr);
  for (size_t i = 0; i != 1000; ++i) ASSERT_EQ(grown[i], 'a');
  EXPECT_EQ(cache.liveBlocks(), 1u);
  cache.release(grown);
  auto* foreignLarge = std::malloc(200 * 1024);
  cache.release(foreignLarge);                                    // not tracked: goes to free()
  EXPECT_EQ(cache.cachedBlocks(), 1u);
}

TEST(LargeBufferCache, CacheSizeIsBounded) {
  LargeBufferCache cache(256 * 1024);
  std::vector<void*> blocks;
  for (int i = 0; i != 4; ++i) blocks.push_back(cache.allocate(128 * 1024));
  for (auto* block : blocks) cache.release(block);
  EXPECT_EQ(cache.cachedBytes(), 256u * 1024);
  EXPECT_EQ(cache.cachedBlocks(), 2u);
}

TEST(LargeBufferCache, CallocAndStrdup) {
  LargeBufferCache cache;
  auto* zeroed = static_cast<unsigned char*>(cache.allocateZeroed(1024, 100));
  ASSERT_NE(zeroed, nullptr);
  for (size_t i = 0; i != 1024 * 100; ++i) ASSERT_EQ(zeroed[i], 0);
  cache.release(zeroed);
  EXPECT_EQ(cache.allocateZeroed(SIZE_MAX / 2, 4), nullptr);
  char* copy = cache.duplicate("hello");
  EXPECT_STREQ(copy, "hello");
  cache.release(copy);
}

static long minorFaults() {
  rusage usage{};
  getrusage(RUSAGE_SELF, &usage);
  return usage.ru_minflt;
}

static RA_Options testOptions() {
  RA_Options options;
  if (const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT")) options.cxn.port = std::stoi(port);
  return options;
}

static std::string testBase() {
  return "buffer-cache-" + std::to_string(getpid()) + "-" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

//  A 1 MiB add used to fault in about 4 MiB of fresh pages (redis-adapter#146). With the
//  buffers reused, steady-state adds fault in almost nothing.
TEST(LargeBufferCache, LargeAddsDoNotFaultInFreshPages) {
  RA adapter(testBase(), testOptions());
  ASSERT_TRUE(adapter.connected());
  const std::vector<uint8_t> payload(1 << 20, 0x5a);
  constexpr int adds = 200;
  constexpr long maxFaultsPerAdd = 32;                            // a 1 MiB payload is 256 pages
  for (int i = 0; i != 20; ++i) ASSERT_TRUE(adapter.addSingleList("direct", payload, {.trim=2}).ok());
  long before = minorFaults();
  for (int i = 0; i != adds; ++i) ASSERT_TRUE(adapter.addSingleList("direct", payload, {.trim=2}).ok());
  const long direct = (minorFaults() - before) / adds;
  EXPECT_LT(direct, maxFaultsPerAdd) << "direct adds fault in " << direct << " pages each";

  auto pipeline = adapter.pipeline();
  const auto batch = [&] {
    for (int i = 0; i != 4; ++i) pipeline.addSingleListView("pipelined", payload, {.trim=8});
    for (const auto& result : pipeline.exec()) ASSERT_TRUE(result.at(0).ok());
  };
  for (int i = 0; i != 5; ++i) batch();
  before = minorFaults();
  for (int i = 0; i != adds / 4; ++i) batch();
  const long pipelined = (minorFaults() - before) / adds;
  EXPECT_LT(pipelined, maxFaultsPerAdd) << "pipelined adds fault in " << pipelined << " pages each";
  adapter.del("direct");
  adapter.del("pipelined");
}
