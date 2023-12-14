#include <gtest/gtest.h>
#include "RedisAdapter.hpp"

using namespace std;
using namespace sw::redis;
using namespace std::chrono;

TEST(RedisAdapter, Connected)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.connected());
}

TEST(RedisAdapter, ExitNotConnected)
{
  RedisAdapter redis("TEST");

  if ( ! redis.connected()) exit(1);
}

TEST(RedisAdapter, Status)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.setStatus("abc", "OK"));

  EXPECT_STREQ(redis.getStatus("abc").c_str(), "OK");
}

TEST(RedisAdapter, Log)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.addLog("log 1"));

  auto log = redis.getLog("-");

  EXPECT_GT(log.size(), 0);

  log = redis.getLogAfter("-");

  EXPECT_GT(log.size(), 0);

  log = redis.getLogBefore();

  EXPECT_GT(log.size(), 0);
}

TEST(RedisAdapter, Setting)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.setSetting("abc", "123"));
  EXPECT_STREQ(redis.getSetting<string>("abc").c_str(), "123");

  EXPECT_TRUE(redis.setSetting("def", 123));
  auto opt_int = redis.getSetting<int>("def");
  EXPECT_TRUE(opt_int.has_value());
  EXPECT_EQ(opt_int.value(), 123);

  EXPECT_TRUE(redis.setSetting("ghi", 1.23f));
  auto opt_flt = redis.getSetting<float>("ghi");
  EXPECT_TRUE(opt_flt.has_value());
  EXPECT_FLOAT_EQ(opt_flt.value(), 1.23);

  EXPECT_TRUE(redis.setSetting<float>("jkl", 1.23));
  opt_flt = redis.getSetting<float>("ghi");
  EXPECT_TRUE(opt_flt.has_value());
  EXPECT_FLOAT_EQ(opt_flt.value(), 1.23);

  vector<float> vf_set = { 1.23, 3.45, 5.67 };
  EXPECT_TRUE(redis.setSettingList("xyz", vf_set));
  auto vf_get = redis.getSettingList<float>("xyz");
  EXPECT_EQ(vf_get.size(), 3);
  EXPECT_FLOAT_EQ(vf_get[2], 5.67);
}

TEST(RedisAdapter, Data)
{
  RedisAdapter redis("TEST");

  EXPECT_GT(redis.addDataSingle("abc", 1.23f).size(), 0);
  float f;
  EXPECT_GT(redis.getDataSingle("abc", f).size(), 0);
  EXPECT_FLOAT_EQ(f, 1.23);

  vector<float> vf_add = { 1.23, 3.45, 5.67 };
  EXPECT_GT(redis.addDataListSingle("def", vf_add).size(), 0);
  vector<float> vf_get;
  EXPECT_GT(redis.getDataListSingle("def", vf_get).size(), 0);
  EXPECT_FLOAT_EQ(vf_get[2], 5.67);
}

TEST(RedisAdapter, Listener)
{
  RedisAdapter redis("TEST");

  bool waiting = true;
  redis.addStatusReader("xyz", [&](const string& base, const string& sub, const ItemStream<Attrs>&)
    {
      waiting = false;
    }
  );
  this_thread::sleep_for(milliseconds(5));
  redis.setStatus("xyz", "OK");

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_FALSE(waiting);
}

TEST(RedisAdapter, PubSub)
{
  RedisAdapter redis("TEST");

  bool waiting = true;
  redis.subscribe("abc", [&](const string& base, const string& sub, const string& msg)
    {
      waiting = false;
    }
  );
  this_thread::sleep_for(milliseconds(5));
  redis.publish("abc", "123");

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_FALSE(waiting);
}
