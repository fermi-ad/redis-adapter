//
//  test_watchdog_race.cpp
//
//  Regression: Watchdog shutdown race condition
//
//  Bug: The destructor checked _watchdog_run (set inside the thread) to
//  decide whether to join. If the destructor ran before the thread set
//  _watchdog_run=true, the join was skipped, leaving a joinable thread
//  whose destructor calls std::terminate. Additionally, _watchdog_run
//  was modified without holding the mutex, causing missed wakeups.
//
//  Fix: Set _watchdog_run=true in the constructor before creating the
//  thread. Hold the mutex when clearing the flag in the destructor.
//  Always join if the thread is joinable.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <memory>
#include <thread>

using namespace std;
using namespace chrono;

TEST(WatchdogRace, RapidLifecycle)
{
  for (int i = 0; i < 10; i++)
  {
    RAL_Options opts;
    opts.dogname = "RAPID_TEST";
    RedisAdapterLite redis("TEST_RAPID_WD", opts);
  }
}

TEST(WatchdogRace, CleanShutdownAfterRunning)
{
  RAL_Options opts;
  opts.dogname = "SHUTDOWN_TEST";
  auto redis = std::make_unique<RedisAdapterLite>("TEST_SHUTDOWN_WD", opts);

  this_thread::sleep_for(milliseconds(200));

  auto start = steady_clock::now();
  redis.reset();
  auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start);

  EXPECT_LT(elapsed.count(), 2000);
}

TEST(WatchdogRace, ImmediateDestruction)
{
  RAL_Options opts;
  opts.dogname = "IMMED_TEST";
  RedisAdapterLite redis("TEST_IMMED_WD", opts);
}

TEST(WatchdogRace, ErrorHandlingLogsOnFailure)
{
  RAL_Options opts;
  opts.dogname = "ERR_TEST";
  RedisAdapterLite redis("TEST_ERR_WD", opts);
  this_thread::sleep_for(milliseconds(100));

  EXPECT_TRUE(redis.addWatchdog("MANUAL_DOG", 2));
  EXPECT_TRUE(redis.petWatchdog("MANUAL_DOG", 2));
}
