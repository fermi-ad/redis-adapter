//
//  test_reader_reference.cpp
//
//  Regression: Reader thread captured &info by reference
//
//  Bug: The reader thread lambda captured &info (a reference to a
//  _readers map element). While safe due to join-before-erase discipline,
//  this was fragile and could lead to use-after-free if the invariant
//  was accidentally broken by future changes.
//
//  Fix: Capture token by value and look up the ReaderInfo inside the
//  lambda, making the safety invariant explicit and self-documenting.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>

using namespace std;
using namespace chrono;

TEST(ReaderReference, AddRemoveRapid)
{
  RedisAdapterLite redis("TEST");
  redis.addInt("t_reader_rapid", 0);

  for (int i = 0; i < 5; i++)
  {
    redis.addReader("t_reader_rapid",
      [](const string&, const string&, const TimeAttrsList&) {});
    this_thread::sleep_for(milliseconds(10));
    redis.removeReader("t_reader_rapid");
  }
}

TEST(ReaderReference, ReaderReceivesAfterRestart)
{
  RedisAdapterLite redis("TEST");
  redis.addInt("t_reader_restart", 0);

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

TEST(ReaderReference, MultipleReadersOnDifferentKeys)
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
