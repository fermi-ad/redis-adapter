//
//  test_watchdog.cpp
//
//  Integration tests for watchdog feature
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <thread>

using namespace std;
using namespace chrono;

TEST(Watchdog, AutoRegistration)
{
  { RedisAdapterLite cleanup("TEST_WD"); cleanup.del("watchdog"); }

  RAL_Options opts;
  opts.dogname = "WDTEST";
  RedisAdapterLite redis("TEST_WD", opts);

  this_thread::sleep_for(milliseconds(100));
  EXPECT_EQ(redis.getWatchdogs().size(), 1u);

  EXPECT_TRUE(redis.addWatchdog("SPOT", 1));

  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2u);

  EXPECT_TRUE(redis.petWatchdog("SPOT", 1));
  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2u);

  this_thread::sleep_for(milliseconds(600));
  auto dogs = redis.getWatchdogs();
  if (dogs.size() == 1u)
    EXPECT_EQ(dogs.size(), 1u);
  else
    GTEST_SKIP() << "HEXPIRE not supported (requires Redis 7.4+), skipping expiration check";
}

TEST(Watchdog, NoDogname)
{
  RedisAdapterLite redis("TEST_WD2");

  this_thread::sleep_for(milliseconds(50));
  auto dogs = redis.getWatchdogs();
  EXPECT_EQ(dogs.size(), 0u);
}
