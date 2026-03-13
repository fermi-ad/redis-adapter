//
//  test_pubsub.cpp
//
//  Integration tests for pub/sub
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>

using namespace std;
using namespace chrono;

TEST(PubSub, BasicMessage)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_msg;

  EXPECT_TRUE(redis.subscribe("t_pubsub",
    [&](const string& base, const string& sub, const string& msg)
    {
      received_msg = msg;
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  EXPECT_TRUE(redis.publish("t_pubsub", "hello world"));

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_msg, "hello world");

  EXPECT_TRUE(redis.unsubscribe("t_pubsub"));
}

TEST(PubSub, MultipleMessages)
{
  RedisAdapterLite redis("TEST");

  atomic<int> count{0};

  EXPECT_TRUE(redis.subscribe("t_pubsub_multi",
    [&](const string&, const string&, const string& msg)
    {
      count++;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  for (int i = 0; i < 5; i++)
    redis.publish("t_pubsub_multi", "msg" + to_string(i));

  for (int i = 0; i < 100 && count < 5; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_EQ(count.load(), 5);
  EXPECT_TRUE(redis.unsubscribe("t_pubsub_multi"));
}

TEST(PubSub, BaseKey)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_base;

  EXPECT_TRUE(redis.subscribe("t_pubsub_bk",
    [&](const string& base, const string& sub, const string& msg)
    {
      received_base = base;
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  EXPECT_TRUE(redis.publish("t_pubsub_bk", "msg"));

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_base, "TEST");

  EXPECT_TRUE(redis.unsubscribe("t_pubsub_bk"));
}
