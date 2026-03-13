//
//  test_base_key.cpp
//
//  Tests for key building, baseKey overrides, and cross-instance access
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

using namespace std;

TEST(BaseKey, DefaultKeyFormat)
{
  RedisAdapterLite redis("MYBASE");
  redis.addInt("mystream", 42);
  int64_t val = 0;
  auto t = redis.getInt("mystream", val);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(val, 42);
  redis.del("mystream");
}

TEST(BaseKey, EmptySubKey)
{
  RedisAdapterLite redis("BASEONLY");
  redis.addInt("", 99);
  int64_t val = 0;
  auto t = redis.getInt("", val);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(val, 99);
  redis.del("");
}

TEST(BaseKey, OverrideBaseKeyOnGet)
{
  RedisAdapterLite redis1("BASE1");
  RedisAdapterLite redis2("BASE2");

  redis1.addString("shared", "from_base1");

  string val;
  auto t = redis2.getString("shared", val, {.baseKey = "BASE1"});
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(val, "from_base1");

  redis1.del("shared");
}

TEST(BaseKey, CrossInstanceCopy)
{
  RedisAdapterLite src("SRC_BASE");
  RedisAdapterLite dst("DST_BASE");

  src.addInt("data", 123);

  dst.copy("data", "data_copy", "SRC_BASE");

  int64_t val = 0;
  auto t = dst.getInt("data_copy", val);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(val, 123);

  src.del("data");
  dst.del("data_copy");
}

TEST(BaseKey, PublishSubscribeWithBaseKey)
{
  RedisAdapterLite redis("PUBBASE");
  atomic<bool> received{false};

  redis.subscribe("chan", [&](const string&, const string&, const string& msg)
  {
    if (!msg.empty()) received = true;
  }, "CUSTOM_BASE");

  this_thread::sleep_for(chrono::milliseconds(100));

  redis.publish("chan", "hello", "CUSTOM_BASE");

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(chrono::milliseconds(5));

  EXPECT_TRUE(received.load());
  redis.unsubscribe("chan", "CUSTOM_BASE");
}
