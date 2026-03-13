//
//  test_empty_range_reconnect.cpp
//
//  Bug: get_forward() and get_reverse() called check_reconnect(0)
//  when an XRANGE/XREVRANGE returned an empty result. This triggered
//  unnecessary reconnection attempts (spawning threads, stopping and
//  restarting all readers) on every empty query.
//
//  Fix: Changed to check_reconnect(_redis.is_connected() ? 1 : 0)
//  so only actual connection failures trigger reconnection.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <chrono>
#include <thread>

using namespace std;
using namespace chrono;

// Query a non-existent key — should return empty without triggering reconnect.
// Verify by checking that the connection is still alive immediately after.
TEST(EmptyRangeReconnect, EmptyRangeDoesNotTriggerReconnect)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  // Query a key that doesn't exist — valid empty result
  RAL_GetArgs args;
  auto result = redis.getStrings("t_nonexistent_key_xyz", args);
  EXPECT_TRUE(result.empty());

  // Connection should still be up — no spurious reconnect
  EXPECT_TRUE(redis.connected());
}

TEST(EmptyRangeReconnect, EmptyReverseRangeDoesNotTriggerReconnect)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  RAL_GetArgs args;
  args.count = 10;
  auto result = redis.getStringsBefore("t_nonexistent_key_xyz", args);
  EXPECT_TRUE(result.empty());

  EXPECT_TRUE(redis.connected());
}

// Verify that data retrieval still works after empty queries
TEST(EmptyRangeReconnect, DataStillAccessibleAfterEmptyQuery)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  // Write some data
  redis.addString("t_range_test", "hello");

  // Empty query on different key
  RAL_GetArgs args;
  auto empty = redis.getStrings("t_nonexistent_abc", args);
  EXPECT_TRUE(empty.empty());

  // Original data should still be accessible
  string val;
  RAL_Time t = redis.getString("t_range_test", val);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(val, "hello");

  redis.del("t_range_test");
}
