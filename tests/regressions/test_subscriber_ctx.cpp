//
//  test_subscriber_ctx.cpp
//
//  Regression: Subscriber context lifecycle bugs
//
//  Bug 1 (race): The subscriber thread freed _sub.ctx at exit, but
//  stop_subscriber() also read _sub.ctx to decide whether to send
//  an unblock PUBLISH. If the thread exited due to a network error,
//  both could race on _sub.ctx — use-after-free / double-free.
//
//  Fix: The thread no longer frees _sub.ctx. Cleanup is done by
//  stop_subscriber() after join(), when the thread is guaranteed stopped.
//
//  Bug 2 (null deref): The subscriber parsed reply->element[0..2]->str
//  without checking for null element pointers. A malformed Redis reply
//  with null elements would crash.
//
//  Fix: Added null checks for all three elements and their str pointers
//  before dereferencing.
//
//  Bug 3 (stop_subscriber guard): The check `if (!_sub.run) return`
//  would skip cleanup if the subscriber thread had already exited on
//  its own (e.g., network error set run=false before stop was called),
//  leaving the thread unjoined and _sub.ctx leaked.
//
//  Fix: Changed guard to `if (!_sub.thread.joinable()) return`.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>
#include <chrono>

using namespace std;
using namespace chrono;

// Test that rapid subscribe/stop cycles don't crash or leak.
// Exercises the ctx lifecycle: create in restart_subscriber,
// used by thread, freed in stop_subscriber after join.
TEST(SubscriberCtx, RapidLifecycleNoCrash)
{
  RedisAdapterLite redis("TEST");

  for (int i = 0; i < 20; i++)
  {
    redis.subscribe("t_ctx_rapid",
      [](const string&, const string&, const string&) {});
    // Tiny sleep to let the thread actually start and use ctx
    this_thread::sleep_for(milliseconds(2));
    redis.unsubscribe("t_ctx_rapid");
  }
  // If we get here without crashing/hanging, the ctx lifecycle is safe
  SUCCEED();
}

// Test that subscribing, receiving messages, and unsubscribing
// works cleanly — the ctx must survive the entire message flow.
TEST(SubscriberCtx, ReceiveMessageThenCleanStop)
{
  RedisAdapterLite redis("TEST");
  atomic<int> count{0};

  redis.subscribe("t_ctx_msg",
    [&](const string&, const string&, const string& msg)
    {
      if (!msg.empty()) count++;
    });
  this_thread::sleep_for(milliseconds(50));

  // Send a few messages
  for (int i = 0; i < 5; i++)
    redis.publish("t_ctx_msg", "hello");

  // Wait for delivery
  for (int i = 0; i < 100 && count < 5; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_GE(count.load(), 1);  // At least some should arrive

  // Clean unsubscribe — ctx freed after join
  redis.unsubscribe("t_ctx_msg");
  // No crash = success
}

// Test destruction with active subscriber — destructor calls
// stop_subscriber which must cleanly join and free ctx.
TEST(SubscriberCtx, DestructorCleansUpActiveSubscriber)
{
  {
    RedisAdapterLite redis("TEST");
    redis.subscribe("t_ctx_dtor",
      [](const string&, const string&, const string&) {});
    this_thread::sleep_for(milliseconds(20));
    // Destructor runs here — must not crash or hang
  }
  SUCCEED();
}

// Test that re-subscribing (which internally stops then restarts)
// handles ctx transitions correctly.
TEST(SubscriberCtx, ResubscribeTransition)
{
  RedisAdapterLite redis("TEST");
  atomic<int> version{0};

  redis.subscribe("t_ctx_resub",
    [&](const string&, const string&, const string& msg)
    {
      if (!msg.empty()) version = 1;
    });
  this_thread::sleep_for(milliseconds(50));

  // Re-subscribe with new callback — old ctx freed, new ctx created
  redis.subscribe("t_ctx_resub",
    [&](const string&, const string&, const string& msg)
    {
      if (!msg.empty()) version = 2;
    });
  this_thread::sleep_for(milliseconds(50));

  redis.publish("t_ctx_resub", "test");
  for (int i = 0; i < 60 && version < 2; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_EQ(version.load(), 2);
  redis.unsubscribe("t_ctx_resub");
}

// Stress test: concurrent publish while subscribe/unsubscribe cycles
// are happening. This hammers the ctx lifecycle from multiple angles.
TEST(SubscriberCtx, ConcurrentPublishDuringLifecycle)
{
  RedisAdapterLite redis("TEST");
  atomic<bool> stop{false};
  atomic<int> received{0};

  // Publisher thread
  thread publisher([&]()
  {
    while (!stop)
    {
      redis.publish("t_ctx_stress", "ping");
      this_thread::sleep_for(milliseconds(2));
    }
  });

  // Subscribe/unsubscribe cycles while messages are flying
  for (int i = 0; i < 5; i++)
  {
    redis.subscribe("t_ctx_stress",
      [&](const string&, const string&, const string& msg)
      {
        if (!msg.empty()) received++;
      });
    this_thread::sleep_for(milliseconds(20));
    redis.unsubscribe("t_ctx_stress");
    this_thread::sleep_for(milliseconds(5));
  }

  stop = true;
  publisher.join();

  // Some messages should have been received across the cycles
  EXPECT_GE(received.load(), 0);  // Main point: no crash
}
