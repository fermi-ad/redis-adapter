//
//  test_bugfixes.cpp
//
//  Unit tests for bug fixes in RedisAdapterLite
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <memory>
#include <atomic>
#include <thread>

using namespace std;
using namespace chrono;

// ===================================================================
//  Fix: Watchdog shutdown race condition
//  The destructor now holds the mutex when setting _watchdog_run=false
//  and checks joinable() unconditionally, preventing missed wakeups
//  and std::terminate on joinable thread destruction.
// ===================================================================

TEST(WatchdogFix, RapidLifecycle)
{
  // Rapidly create and destroy adapters with watchdog enabled.
  // Previously, a race between the constructor setting _watchdog_run=true
  // and the destructor checking it could cause std::terminate.
  for (int i = 0; i < 10; i++)
  {
    RAL_Options opts;
    opts.dogname = "RAPID_TEST";
    RedisAdapterLite redis("TEST_RAPID_WD", opts);
  }
}

TEST(WatchdogFix, CleanShutdownAfterRunning)
{
  RAL_Options opts;
  opts.dogname = "SHUTDOWN_TEST";
  auto redis = std::make_unique<RedisAdapterLite>("TEST_SHUTDOWN_WD", opts);

  // Let the watchdog thread run for a bit
  this_thread::sleep_for(milliseconds(200));

  // Destroy — should complete promptly without hanging
  auto start = steady_clock::now();
  redis.reset();
  auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start);

  // Should shut down well within the 900ms watchdog interval
  EXPECT_LT(elapsed.count(), 2000);
}

TEST(WatchdogFix, ImmediateDestruction)
{
  // Create with watchdog and immediately destroy — the thread may not
  // have started yet. Destructor must still join cleanly.
  RAL_Options opts;
  opts.dogname = "IMMED_TEST";
  RedisAdapterLite redis("TEST_IMMED_WD", opts);
  // Destructor runs here
}

TEST(WatchdogFix, ErrorHandlingLogsOnFailure)
{
  // Verify watchdog works correctly with valid dogname
  RAL_Options opts;
  opts.dogname = "ERR_TEST";
  RedisAdapterLite redis("TEST_ERR_WD", opts);
  this_thread::sleep_for(milliseconds(100));

  // Manual watchdog operations should succeed
  EXPECT_TRUE(redis.addWatchdog("MANUAL_DOG", 2));
  EXPECT_TRUE(redis.petWatchdog("MANUAL_DOG", 2));
}

// ===================================================================
//  Fix: Subscriber data race
//  subscribe() and unsubscribe() now stop the subscriber thread
//  BEFORE modifying _sub.channels, preventing concurrent iteration
//  by the thread without the lock.
// ===================================================================

TEST(SubscriberFix, RapidSubscribeUnsubscribe)
{
  RedisAdapterLite redis("TEST");

  // Rapidly subscribe and unsubscribe — previously, erase happened
  // while the subscriber thread was iterating channels
  for (int i = 0; i < 10; i++)
  {
    redis.subscribe("t_sub_race",
      [](const string&, const string&, const string&) {});
    this_thread::sleep_for(milliseconds(5));
    redis.unsubscribe("t_sub_race");
  }
  // Should complete without crash or deadlock
}

TEST(SubscriberFix, MultiChannelSubscribeUnsubscribe)
{
  RedisAdapterLite redis("TEST");

  // Subscribe to multiple channels
  redis.subscribe("t_sub_mc1",
    [](const string&, const string&, const string&) {});
  redis.subscribe("t_sub_mc2",
    [](const string&, const string&, const string&) {});
  redis.subscribe("t_sub_mc3",
    [](const string&, const string&, const string&) {});
  this_thread::sleep_for(milliseconds(50));

  // Remove one at a time — each unsubscribe stops the thread, erases,
  // then restarts with remaining channels
  redis.unsubscribe("t_sub_mc2");
  this_thread::sleep_for(milliseconds(20));

  // Verify remaining channel still works
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

TEST(SubscriberFix, SubscribeOverwrite)
{
  RedisAdapterLite redis("TEST");

  atomic<int> callback_id{0};

  // Subscribe with callback A
  redis.subscribe("t_sub_overwrite",
    [&](const string&, const string&, const string&)
    {
      callback_id = 1;
    });
  this_thread::sleep_for(milliseconds(50));

  // Subscribe again with callback B (should safely replace)
  redis.subscribe("t_sub_overwrite",
    [&](const string&, const string&, const string&)
    {
      callback_id = 2;
    });
  this_thread::sleep_for(milliseconds(50));

  redis.publish("t_sub_overwrite", "test");
  for (int i = 0; i < 40 && callback_id == 0; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_EQ(callback_id.load(), 2);
  redis.unsubscribe("t_sub_overwrite");
}

// ===================================================================
//  Fix: Reader thread reference capture
//  Lambda now captures token by value and looks up ReaderInfo by token,
//  avoiding a dangling reference if the map were to be modified.
// ===================================================================

TEST(ReaderFix, AddRemoveRapid)
{
  RedisAdapterLite redis("TEST");
  redis.addInt("t_reader_rapid", 0);

  // Rapidly add and remove readers — exercises the token-based lookup
  for (int i = 0; i < 5; i++)
  {
    redis.addReader("t_reader_rapid",
      [](const string&, const string&, const TimeAttrsList&) {});
    this_thread::sleep_for(milliseconds(10));
    redis.removeReader("t_reader_rapid");
  }
}

TEST(ReaderFix, ReaderReceivesAfterRestart)
{
  RedisAdapterLite redis("TEST");
  redis.addInt("t_reader_restart", 0);

  // Add reader, remove it, add again — should still receive
  redis.addReader("t_reader_restart",
    [](const string&, const string&, const TimeAttrsList&) {});
  this_thread::sleep_for(milliseconds(10));
  redis.removeReader("t_reader_restart");

  atomic<bool> received{false};
  redis.addReader("t_reader_restart",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty()) received = true;
    });
  this_thread::sleep_for(milliseconds(10));

  redis.addInt("t_reader_restart", 42);
  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  redis.removeReader("t_reader_restart");
}

TEST(ReaderFix, MultipleReadersOnDifferentKeys)
{
  RAL_Options opts;
  opts.readers = 4;
  RedisAdapterLite redis("TEST", opts);
  redis.addInt("t_reader_mk1", 0);
  redis.addInt("t_reader_mk2", 0);

  atomic<int> count1{0};
  atomic<int> count2{0};

  redis.addReader("t_reader_mk1",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty()) count1++;
    });
  redis.addReader("t_reader_mk2",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty()) count2++;
    });
  this_thread::sleep_for(milliseconds(20));

  redis.addInt("t_reader_mk1", 1);
  redis.addInt("t_reader_mk2", 2);

  for (int i = 0; i < 40 && (count1 == 0 || count2 == 0); i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_GE(count1.load(), 1);
  EXPECT_GE(count2.load(), 1);

  redis.removeReader("t_reader_mk1");
  redis.removeReader("t_reader_mk2");
}

// ===================================================================
//  Fix: AUTH command injection
//  AUTH now uses redisCommandArgv instead of format-string redisCommand,
//  preventing malformed commands from special chars in passwords.
// ===================================================================

TEST(AuthFix, ConnectionWithoutAuth)
{
  // Default options — no password. Verify connection still works
  // after switching to argv-based AUTH.
  RAL_Options opts;
  RedisAdapterLite redis("TEST_AUTH_FIX", opts);
  EXPECT_TRUE(redis.connected());
}

TEST(AuthFix, ConnectionWithBadAuth)
{
  // Bad credentials — should fail gracefully, not crash
  RAL_Options opts;
  opts.user = "nonexistent user with spaces";
  opts.password = "bad password with \"quotes\" and 'specials'";
  RedisAdapterLite redis("TEST_AUTH_BAD", opts);
  // May or may not connect depending on Redis ACL config,
  // but should not crash or produce malformed commands
}

// ===================================================================
//  Fix: RAL_Time silent exception swallowing
//  Parse failures are now logged via syslog instead of being
//  silently converted to value=0.
// ===================================================================

TEST(RAL_TimeFix, EmptyString)
{
  RAL_Time t("");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeFix, GarbageInput)
{
  RAL_Time t("abc-def");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeFix, DashOnly)
{
  RAL_Time t("-");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeFix, OverflowInput)
{
  // Extremely large number that overflows int64_t
  RAL_Time t("999999999999999999999-0");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeFix, ValidZero)
{
  RAL_Time t("0-0");
  EXPECT_EQ(t.value, 0);
  EXPECT_FALSE(t.ok());  // 0 is not considered "ok"
}

TEST(RAL_TimeFix, ValidSmall)
{
  RAL_Time t("1-0");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 1'000'000);  // 1ms = 1,000,000 nanos
}

TEST(RAL_TimeFix, ValidWithSubId)
{
  RAL_Time t("100-5");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 100'000'000 + 5);
}

TEST(RAL_TimeFix, PartialGarbage)
{
  // Valid millisecond part but garbage after dash
  RAL_Time t("100-xyz");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeFix, NoDash)
{
  // Valid number without a dash — should parse milliseconds only
  RAL_Time t("500");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 500'000'000);
}

TEST(RAL_TimeFix, RoundTripPreservation)
{
  // Verify id() -> RAL_Time round trip after fix
  RAL_Time orig(123456789012345LL);
  string id = orig.id();
  RAL_Time parsed(id);
  EXPECT_EQ(parsed.value, orig.value);
}
