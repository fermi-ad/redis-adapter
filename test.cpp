#include <gtest/gtest.h>
#include "RedisAdapter.hpp"

using namespace std;
using namespace sw::redis;
using namespace std::chrono;

using GDA = RedisAdapter::GetDataArgs;

TEST(RedisAdapter, Connected)
{
  RedisAdapter redis("TEST");

//  provide a pass/fail indication if Redis server is available
  EXPECT_TRUE(redis.connected());
}

TEST(RedisAdapter, ExitNotConnected)
{
  RedisAdapter redis("TEST");

//  abort tests if Redis server is not available
  if ( ! redis.connected()) exit(1);
}

TEST(RedisAdapter, Status)
{
  RedisAdapter redis("TEST");

  //  test local get/set
  EXPECT_TRUE(redis.setStatus("abc", "OK"));
  EXPECT_STREQ(redis.getStatus("abc").c_str(), "OK");

  //  test foreign get fails (can't do foreign set)
  EXPECT_EQ(redis.getStatus("abc", "xyz").size(), 0);
}

TEST(RedisAdapter, Log)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.addLog("log 1"));
  EXPECT_TRUE(redis.addLog("log 2"));

  //  get most recent 2 entries
  auto log = redis.getLogBefore(2);
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries starting at next-to-last entry
  log = redis.getLog(log.front().first);
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries after starting at next-to-last entry
  log = redis.getLogAfter(log.front().first);
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries before starting at last entry
  log = redis.getLogBefore(log.back().first, 2);
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");
}

TEST(RedisAdapter, Setting)
{
  RedisAdapter redis("TEST");

  //  set/get string setting
  EXPECT_TRUE(redis.setSetting("abc", "xxx"));
  EXPECT_STREQ(redis.getSetting<string>("abc").c_str(), "xxx");

  //  get non-existent string setting
  EXPECT_STREQ(redis.getSetting<string>("def").c_str(), "");

  //  set/get int setting
  EXPECT_TRUE(redis.setSetting("abc", 123));
  auto opt_int = redis.getSetting<int>("abc");
  EXPECT_TRUE(opt_int.has_value());
  EXPECT_EQ(opt_int.value(), 123);

  //  get non-existent int setting
  opt_int = redis.getSetting<int>("def");
  EXPECT_FALSE(opt_int.has_value());

  //  set/get float setting
  EXPECT_TRUE(redis.setSetting("abc", 1.23f));    //  Note value MUST have 'f' suffix else it's a double
  auto opt_flt = redis.getSetting<float>("abc");
  EXPECT_TRUE(opt_flt.has_value());
  EXPECT_FLOAT_EQ(opt_flt.value(), 1.23);         //  Here it doesn't matter, we are just comparing

  //  set/get float setting
  EXPECT_TRUE(redis.setSetting<float>("abc", 1.23));  //  Here it's OK we are calling specialization <float>
  opt_flt = redis.getSetting<float>("abc");
  EXPECT_TRUE(opt_flt.has_value());
  EXPECT_FLOAT_EQ(opt_flt.value(), 1.23);             //  Here it doesn't matter, we are just comparing

  //  set/get double setting
  EXPECT_TRUE(redis.setSettingDouble("abc", 1.23));
  auto opt_dbl = redis.getSetting<double>("abc");
  EXPECT_TRUE(opt_dbl.has_value());
  EXPECT_DOUBLE_EQ(opt_dbl.value(), 1.23);

  //  set/get vector of floats
  vector<float> vf = { 1.23, 3.45, 5.67 };
  EXPECT_TRUE(redis.setSettingList("abc", vf));
  vf = redis.getSettingList<float>("abc");
  EXPECT_EQ(vf.size(), 3);
  EXPECT_FLOAT_EQ(vf[0], 1.23);
  EXPECT_FLOAT_EQ(vf[1], 3.45);
  EXPECT_FLOAT_EQ(vf[2], 5.67);
}

TEST(RedisAdapter, DataSingle)
{
  RedisAdapter redis("TEST");

  //  set/get string single element
  EXPECT_GT(redis.addDataSingle("abc", "xxx").size(), 0);
  string s;
  EXPECT_GT(redis.getDataSingle("abc", s).size(), 0);
  EXPECT_STREQ(s.c_str(), "xxx");

  //  set/get float single element
  EXPECT_GT(redis.addDataSingle("abc", 1.23f).size(), 0);   //  Note value MUST have 'f' suffix else it's a double
  float f = 0;
  EXPECT_GT(redis.getDataSingle("abc", f).size(), 0);
  EXPECT_FLOAT_EQ(f, 1.23);                                 //  Here it doesn't matter, we are just comparing

  //  set/get float single element
  EXPECT_GT(redis.addDataSingle<float>("abc", 1.23).size(), 0);   //  Here it's OK we are calling specialization <float>
  EXPECT_GT(redis.getDataSingle("abc", f).size(), 0);
  EXPECT_FLOAT_EQ(f, 1.23);                                       //  Here it doesn't matter, we are just comparing

  //  set/get double single element
  EXPECT_GT(redis.addDataDouble("abc", 1.23).size(), 0);
  double d = 0;
  EXPECT_GT(redis.getDataSingle("abc", d).size(), 0);
  EXPECT_DOUBLE_EQ(d, 1.23);

  //  set/get float vector single element
  vector<float> vf = { 1.23, 3.45, 5.67 };
  EXPECT_GT(redis.addDataListSingle("abc", vf).size(), 0);
  vf.clear();
  EXPECT_GT(redis.getDataListSingle("abc", vf).size(), 0);
  EXPECT_FLOAT_EQ(vf[0], 1.23);
  EXPECT_FLOAT_EQ(vf[1], 3.45);
  EXPECT_FLOAT_EQ(vf[2], 5.67);
}

TEST(RedisAdapter, Data)
{
  RedisAdapter redis("TEST");

  //  set/get data
  string idA = redis.addDataSingle("abc", "xxx");
  EXPECT_GT(idA.size(), 0);
  string idB = redis.addDataSingle("abc", "yyy");
  EXPECT_GT(idB.size(), 0);
  auto is_str = redis.getData<string>("abc", idA, idB);
  EXPECT_EQ(is_str.size(), 2);
  EXPECT_GT(is_str.at(0).first.size(), 0);
  EXPECT_STREQ(is_str.at(0).second.c_str(), "xxx");
  EXPECT_GT(is_str.at(1).first.size(), 0);
  EXPECT_STREQ(is_str.at(1).second.c_str(), "yyy");

  //  add multiple data
  ItemStream<int> is_int = {{ "", 1 }, { "", 2 }, { "", 3 }};
  auto ids = redis.addData("abc", is_int);
  EXPECT_EQ(ids.size(), 3);
  EXPECT_GT(ids[0].size(), 0);
  EXPECT_GT(ids[1].size(), 0);
  EXPECT_GT(ids[2].size(), 0);

  //  get data after
  is_int = redis.getDataAfter<int>("abc", GDA{ .minID = ids[0], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_GT(is_int.at(0).first.size(), 0);
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_GT(is_int.at(1).first.size(), 0);
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_GT(is_int.at(2).first.size(), 0);
  EXPECT_EQ(is_int.at(2).second, 3);

  //  get data before
  is_int = redis.getDataBefore<int>("abc", GDA{ .maxID = ids[2], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_GT(is_int.at(0).first.size(), 0);
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_GT(is_int.at(1).first.size(), 0);
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_GT(is_int.at(2).first.size(), 0);
  EXPECT_EQ(is_int.at(2).second, 3);

  //  add/get Attrs data
  ItemStream<Attrs> is_at = {{ "", {{ "a", "1" }, { "b", "2" }}}};
  ids = redis.addData("abc", is_at);
  EXPECT_EQ(ids.size(), 1);
  EXPECT_GT(ids[0].size(), 0);
  is_at = redis.getData<Attrs>("abc", ids[0], ids[0]);
  EXPECT_EQ(is_at.size(), 1);
  EXPECT_STREQ(is_at.at(0).first.c_str(), ids[0].c_str());
  EXPECT_GT(is_at.at(0).second.count("a"), 0);
  EXPECT_STREQ(is_at.at(0).second.at("a").c_str(), "1");
  EXPECT_GT(is_at.at(0).second.count("b"), 0);
  EXPECT_STREQ(is_at.at(0).second.at("b").c_str(), "2");
}

TEST(RedisAdapter, DataList)
{
  RedisAdapter redis("TEST");

  //  add float vectors
  ItemStream<vector<float>> is_vf = {{ "", { 1.1, 1.2, 1.3 }}, { "", { 2.1, 2.2, 2.3 }}};
  auto ids = redis.addDataList("abc", is_vf);
  EXPECT_EQ(ids.size(), 2);
  EXPECT_GT(ids[0].size(), 0);
  EXPECT_GT(ids[1].size(), 0);

  //  get float vector data
  is_vf = redis.getDataList<float>("abc", ids[0], ids[1]);
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_STREQ(is_vf.at(0).first.c_str(), ids[0].c_str());
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_STREQ(is_vf.at(1).first.c_str(), ids[1].c_str());
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data before
  is_vf = redis.getDataListBefore<float>("abc", GDA{.maxID = ids[1], .count = 2});
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_STREQ(is_vf.at(0).first.c_str(), ids[0].c_str());
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_STREQ(is_vf.at(1).first.c_str(), ids[1].c_str());
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data after
  is_vf = redis.getDataListAfter<float>("abc", GDA{.minID = ids[0], .count = 2});
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_STREQ(is_vf.at(0).first.c_str(), ids[0].c_str());
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_STREQ(is_vf.at(1).first.c_str(), ids[1].c_str());
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);
}

TEST(RedisAdapter, StatusListener)
{
  RedisAdapter redis("TEST");

  //  this status should not be seen
  EXPECT_TRUE(redis.setStatus("xyz", "FAIL"));

  //  add status reader
  bool waiting = true;
  EXPECT_TRUE(redis.addStatusReader("xyz", [&](const string& base, const string& sub, const ItemStream<Attrs>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first.size(), 0);
      EXPECT_GT(ats[0].second.count("_"), 0);             //  sadness :(
      EXPECT_STREQ(ats[0].second.at("_").c_str(), "OK");  //  sadness :(
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger status reader
  EXPECT_TRUE(redis.setStatus("xyz", "OK"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeStatusReader("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  waiting = true;
  EXPECT_TRUE(redis.setStatus("xyz", "FAIL"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, LogListener)
{
  RedisAdapter redis("TEST");

  //  this msg should not be seen
  EXPECT_TRUE(redis.addLog("log 1"));

  //  add log reader
  bool waiting = true;
  EXPECT_TRUE(redis.addLogReader([&](const string& base, const string& sub, const ItemStream<Attrs>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_EQ(sub.size(), 0);
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first.size(), 0);
      EXPECT_GT(ats[0].second.count("_"), 0);                 //  sadness :(
      EXPECT_STREQ(ats[0].second.at("_").c_str(), "log 2");   //  sadness :(
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger log reader
  EXPECT_TRUE(redis.addLog("log 2"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeLogReader());
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  waiting = true;
  EXPECT_TRUE(redis.addLog("log 3"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, SettingListener)
{
  RedisAdapter redis("TEST");

  //  this status should not be seen
  EXPECT_TRUE(redis.setSetting("xyz", "FAIL"));

  //  add status reader
  bool waiting = true;
  EXPECT_TRUE(redis.addSettingReader("xyz", [&](const string& base, const string& sub, const ItemStream<Attrs>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first.size(), 0);
      EXPECT_GT(ats[0].second.count("_"), 0);             //  sadness :(
      EXPECT_STREQ(ats[0].second.at("_").c_str(), "OK");  //  sadness :(
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger status reader
  EXPECT_TRUE(redis.setSetting("xyz", "OK"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeSettingReader("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  waiting = true;
  EXPECT_TRUE(redis.setSetting("xyz", "FAIL"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, DataListener)
{
  RedisAdapter redis("TEST");

  //  this status should not be seen
  EXPECT_GT(redis.addDataSingle("xyz", "FAIL").size(), 0);

  //  add status reader
  bool waiting = true;
  EXPECT_TRUE(redis.addDataReader("xyz", [&](const string& base, const string& sub, const ItemStream<Attrs>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first.size(), 0);
      EXPECT_GT(ats[0].second.count("_"), 0);             //  sadness :(
      EXPECT_STREQ(ats[0].second.at("_").c_str(), "OK");  //  sadness :(
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger status reader
  EXPECT_GT(redis.addDataSingle("xyz", "OK").size(), 0);

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeDataReader("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  waiting = true;
  EXPECT_GT(redis.addDataSingle("xyz", "FAIL").size(), 0);

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, PubSub)
{
  RedisAdapter redis("TEST");

  //  this publish should not be seen
  EXPECT_TRUE(redis.publish("xyz", "000"));

  //  subscribe
  bool waiting = true;
  EXPECT_TRUE(redis.subscribe("xyz", [&](const string& base, const string& sub, const string& msg)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_STREQ(msg.c_str(), "123");
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger subscription
  EXPECT_TRUE(redis.publish("xyz", "123"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  dont trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("zzz", "000"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);

  //  unsubscribe
  EXPECT_TRUE(redis.unsubscribe("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("xyz", "000"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, Utility)
{
  RedisAdapter redis("TEST");

  //  copyKey

  //  deleteKey

  //  getTimespec
  auto maybe = redis.getTimespec();
  EXPECT_TRUE(maybe.has_value());
  EXPECT_GT(maybe.value().tv_sec, 0);
  EXPECT_GT(maybe.value().tv_nsec, 0);
}
