//
//  test_subscribe_argv.cpp
//
//  Bug: The subscriber thread used format-string redisCommand for
//  SUBSCRIBE and the stop path used format-string PUBLISH. Channel
//  names containing '%' followed by format specifiers (s, b, d)
//  would be misinterpreted by hiredis's format parser.
//
//  Fix: Switched both to argv-based redisCommandArgv for binary safety,
//  consistent with all other Redis commands in the codebase.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>
#include <chrono>

using namespace std;
using namespace chrono;

// Subscribe and publish on a channel name containing % characters
// that would have been misinterpreted as format specifiers.
TEST(SubscribeArgv, ChannelWithPercentChars)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  atomic<int> count{0};
  string tricky_channel = "t_%s_%d_%b_test";

  bool ok = redis.subscribe(tricky_channel,
    [&](const string&, const string&, const string& msg)
    {
      if (msg == "payload") count++;
    });
  ASSERT_TRUE(ok);
  this_thread::sleep_for(milliseconds(50));

  redis.publish(tricky_channel, "payload");

  for (int i = 0; i < 100 && count < 1; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_GE(count.load(), 1);
  redis.unsubscribe(tricky_channel);
}

// Verify clean unsubscribe (stop path PUBLISH) with special chars
TEST(SubscribeArgv, UnsubscribeWithPercentChars)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  string tricky_channel = "t_unsub_%s_test";

  redis.subscribe(tricky_channel,
    [](const string&, const string&, const string&) {});
  this_thread::sleep_for(milliseconds(20));

  // Unsubscribe exercises the stop path PUBLISH with the tricky name
  redis.unsubscribe(tricky_channel);
  // No crash = success
  SUCCEED();
}
