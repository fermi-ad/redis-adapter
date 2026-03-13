//
//  test_reader.cpp
//
//  Integration tests for stream readers and defer
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>

using namespace std;
using namespace chrono;

TEST(Reader, Double)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};

  EXPECT_TRUE(redis.addDouble("t_reader", 0.0).ok());

  EXPECT_TRUE(redis.addReader("t_reader",
    [&](const string& base, const string& sub, const TimeAttrsList& data)
    {
      received = true;
      EXPECT_EQ(base, "TEST");
      EXPECT_EQ(sub, "t_reader");
      EXPECT_GT(data.size(), 0u);
      auto val = ral_to_double(data[0].second);
      EXPECT_TRUE(val.has_value());
      EXPECT_DOUBLE_EQ(*val, 3.14);
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addDouble("t_reader", 3.14).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());

  EXPECT_TRUE(redis.removeReader("t_reader"));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addDouble("t_reader", 0.0).ok());

  for (int i = 0; i < 20 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_FALSE(received.load());
}

TEST(Reader, Int)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  int64_t received_val = 0;

  EXPECT_TRUE(redis.addInt("t_reader_i", 0).ok());

  EXPECT_TRUE(redis.addReader("t_reader_i",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty())
      {
        auto val = ral_to_int(data[0].second);
        if (val) received_val = *val;
      }
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addInt("t_reader_i", 42).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_val, 42);

  EXPECT_TRUE(redis.removeReader("t_reader_i"));
}

TEST(Reader, String)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_val;

  EXPECT_TRUE(redis.addString("t_reader_s", "init").ok());

  EXPECT_TRUE(redis.addReader("t_reader_s",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty())
      {
        auto val = ral_to_string(data[0].second);
        if (val) received_val = *val;
      }
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addString("t_reader_s", "payload").ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_val, "payload");

  EXPECT_TRUE(redis.removeReader("t_reader_s"));
}

TEST(Reader, Defer)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};

  EXPECT_TRUE(redis.setDeferReaders(true));

  EXPECT_TRUE(redis.addReader("t_defer",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addInt("t_defer", 1).ok());

  for (int i = 0; i < 20 && !received; i++)
    this_thread::sleep_for(milliseconds(5));
  EXPECT_FALSE(received.load());

  EXPECT_TRUE(redis.setDeferReaders(false));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addInt("t_defer", 2).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));
  EXPECT_TRUE(received.load());
}
