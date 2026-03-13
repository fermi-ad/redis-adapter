//
//  test_subscriber_deadlock.cpp
//
//  Regression: Subscriber thread deadlock and data race
//
//  Bug: The subscriber thread acquired _sub_mutex to look up callbacks
//  in _sub.channels during message dispatch. Meanwhile, subscribe() and
//  unsubscribe() held _sub_mutex and called stop_subscriber() which
//  tried to join the thread — deadlock. Additionally, unsubscribe()
//  erased channels before stopping the thread, causing a data race
//  with the thread iterating channels without the lock.
//
//  Fix: The subscriber thread now uses a snapshot (copy) of the channels
//  map, eliminating its need for _sub_mutex entirely. subscribe() and
//  unsubscribe() stop the thread before modifying channels.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>

using namespace std;
using namespace chrono;

TEST(SubscriberDeadlock, RapidSubscribeUnsubscribe)
{
  RedisAdapterLite redis("TEST");

  for (int i = 0; i < 10; i++)
  {
    redis.subscribe("t_sub_race",
      [](const string&, const string&, const string&) {});
    this_thread::sleep_for(milliseconds(5));
    redis.unsubscribe("t_sub_race");
  }
}

TEST(SubscriberDeadlock, MultiChannelSubscribeUnsubscribe)
{
  RedisAdapterLite redis("TEST");

  redis.subscribe("t_sub_mc1",
    [](const string&, const string&, const string&) {});
  redis.subscribe("t_sub_mc2",
    [](const string&, const string&, const string&) {});
  redis.subscribe("t_sub_mc3",
    [](const string&, const string&, const string&) {});
  this_thread::sleep_for(milliseconds(50));

  redis.unsubscribe("t_sub_mc2");
  this_thread::sleep_for(milliseconds(20));

  atomic<bool> received{false};
  redis.unsubscribe("t_sub_mc1");
  redis.subscribe("t_sub_mc1",
    [&](const string&, const string&, const string& msg)
    {
      received = true;
    });
  this_thread::sleep_for(milliseconds(50));

  redis.publish("t_sub_mc1", "test");
  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));
  EXPECT_TRUE(received.load());

  redis.unsubscribe("t_sub_mc1");
  redis.unsubscribe("t_sub_mc3");
}

TEST(SubscriberDeadlock, SubscribeOverwrite)
{
  RedisAdapterLite redis("TEST");

  atomic<int> callback_id{0};

  redis.subscribe("t_sub_overwrite",
    [&](const string&, const string&, const string& msg)
    {
      if (!msg.empty()) callback_id = 1;
    });
  this_thread::sleep_for(milliseconds(50));

  redis.subscribe("t_sub_overwrite",
    [&](const string&, const string&, const string& msg)
    {
      if (!msg.empty()) callback_id = 2;
    });
  this_thread::sleep_for(milliseconds(200));

  redis.publish("t_sub_overwrite", "test");
  for (int i = 0; i < 60 && callback_id == 0; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_EQ(callback_id.load(), 2);
  redis.unsubscribe("t_sub_overwrite");
}
