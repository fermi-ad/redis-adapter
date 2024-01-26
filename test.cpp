#include <gtest/gtest.h>
#include "RedisAdapter.hpp"

using namespace std;
using namespace sw::redis;
using namespace std::chrono;

using RA = RedisAdapter;

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
  EXPECT_TRUE(redis.setStatus("OK"));
  EXPECT_STREQ(redis.getStatus().c_str(), "OK");
}

TEST(RedisAdapter, Log)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.addLog("log 1"));
  EXPECT_TRUE(redis.addLog("log 2"));

  //  get most recent 2 entries
  auto log = redis.getLogBefore({ .count=2 });
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries starting at next-to-last entry
  log = redis.getLog({ .minTime = log.front().first });
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries after starting at next-to-last entry
  log = redis.getLogAfter({ .minTime = log.front().first });
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");

  //  get entries before starting at last entry
  log = redis.getLogBefore({ .maxTime=log.back().first, .count=2 });
  EXPECT_EQ(log.size(), 2);
  EXPECT_STREQ(log.front().second.c_str(), "log 1");
  EXPECT_STREQ(log.back().second.c_str(), "log 2");
}

TEST(RedisAdapter, DataSingle)
{
  RedisAdapter redis("TEST");

  //  set/get string single element
  EXPECT_GT(redis.addStreamSingle("abc", "xxx"), 0);
  string s;
  EXPECT_GT(redis.getStreamSingle("abc", s), 0);
  EXPECT_STREQ(s.c_str(), "xxx");

  //  set/get float single element
  EXPECT_GT(redis.addStreamSingle("abc", 1.23f), 0);  //  Note value MUST have 'f' suffix else it's a double
  float f = 0;
  EXPECT_GT(redis.getStreamSingle("abc", f), 0);
  EXPECT_FLOAT_EQ(f, 1.23);                         //  Here it doesn't matter, we are just comparing

  //  set/get float single element
  EXPECT_GT(redis.addStreamSingle<float>("abc", 1.23), 0);  //  Here it's OK we are calling specialization <float>
  EXPECT_GT(redis.getStreamSingle("abc", f), 0);
  EXPECT_FLOAT_EQ(f, 1.23);                               //  Here it doesn't matter, we are just comparing

  //  set/get double single element
  EXPECT_GT(redis.addStreamSingleDouble("abc", 1.23), 0);
  double d = 0;
  EXPECT_GT(redis.getStreamSingle("abc", d), 0);
  EXPECT_DOUBLE_EQ(d, 1.23);

  //  set/get float vector single element
  vector<float> vf = { 1.23, 3.45, 5.67 };
  EXPECT_GT(redis.addStreamSingleList("abc", vf), 0);
  vf.clear();
  EXPECT_GT(redis.getStreamListSingle("abc", vf), 0);
  EXPECT_EQ(vf.size(), 3);
  EXPECT_FLOAT_EQ(vf[0], 1.23);
  EXPECT_FLOAT_EQ(vf[1], 3.45);
  EXPECT_FLOAT_EQ(vf[2], 5.67);

  //  set/get int array single element (also span if c++20)
  array<int, 3> ai = { 1, 2, 3 };
  EXPECT_GT(redis.addStreamSingleList("abc", ai), 0);
  //  note it comes back as a vector
  vector<int> vi;
  EXPECT_GT(redis.getStreamListSingle("abc", vi), 0);
  EXPECT_EQ(vi.size(), 3);
  EXPECT_EQ(vi[0], 1);
  EXPECT_EQ(vi[1], 2);
  EXPECT_EQ(vi[2], 3);
}

TEST(RedisAdapter, Data)
{
  RedisAdapter redis("TEST");

  //  set/get data
  uint64_t idA = redis.addStreamSingle("abc", "xxx");
  EXPECT_GT(idA, 0);
  uint64_t idB = redis.addStreamSingle("abc", "yyy");
  EXPECT_GT(idB, 0);
  auto is_str = redis.getStream<string>("abc", { .minTime=idA, .maxTime=idB });
  EXPECT_EQ(is_str.size(), 2);
  EXPECT_GT(is_str.at(0).first, 0);
  EXPECT_STREQ(is_str.at(0).second.c_str(), "xxx");
  EXPECT_GT(is_str.at(1).first, 0);
  EXPECT_STREQ(is_str.at(1).second.c_str(), "yyy");

  //  add multiple data
  RA::TimeValList<int> is_int = {{ 0, 1 }, { 0, 2 }, { 0, 3 }};
  auto ids = redis.addStream("abc", is_int);
  EXPECT_EQ(ids.size(), 3);
  EXPECT_GT(ids[0], 0);
  EXPECT_GT(ids[1], 0);
  EXPECT_GT(ids[2], 0);

  //  get data after
  is_int = redis.getStreamAfter<int>("abc", { .minTime = ids[0], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_GT(is_int.at(0).first, 0);
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_GT(is_int.at(1).first, 0);
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_GT(is_int.at(2).first, 0);
  EXPECT_EQ(is_int.at(2).second, 3);

  //  get data before
  is_int = redis.getStreamBefore<int>("abc", { .maxTime = ids[2], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_GT(is_int.at(0).first, 0);
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_GT(is_int.at(1).first, 0);
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_GT(is_int.at(2).first, 0);
  EXPECT_EQ(is_int.at(2).second, 3);

  //  add/get Attrs data
  RA::TimeValList<RA::Attrs> is_at = {{ 0, {{ "a", "1" }, { "b", "2" }}}};
  ids = redis.addStream("abc", is_at);
  EXPECT_EQ(ids.size(), 1);
  EXPECT_GT(ids[0], 0);
  is_at = redis.getStream<RA::Attrs>("abc", { .minTime=ids[0], .maxTime=ids[0] });
  EXPECT_EQ(is_at.size(), 1);
  EXPECT_EQ(is_at.at(0).first, ids[0]);
  EXPECT_GT(is_at.at(0).second.count("a"), 0);
  EXPECT_STREQ(is_at.at(0).second.at("a").c_str(), "1");
  EXPECT_GT(is_at.at(0).second.count("b"), 0);
  EXPECT_STREQ(is_at.at(0).second.at("b").c_str(), "2");
}

TEST(RedisAdapter, DataList)
{
  RedisAdapter redis("TEST");

  //  add float vectors
  RA::TimeValList<vector<float>> is_vf = {{ 0, { 1.1, 1.2, 1.3 }}, { 0, { 2.1, 2.2, 2.3 }}};
  auto ids = redis.addStreamList("abc", is_vf);
  EXPECT_EQ(ids.size(), 2);
  EXPECT_GT(ids[0], 0);
  EXPECT_GT(ids[1], 0);

  //  get float vector data
  is_vf = redis.getStreamList<float>("abc", { .minTime=ids[0], .maxTime=ids[1] });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).first, ids[0]);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).first, ids[1]);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data before
  is_vf = redis.getStreamListBefore<float>("abc", { .maxTime = ids[1], .count = 2 });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).first, ids[0]);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).first, ids[1]);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data after
  is_vf = redis.getStreamListAfter<float>("abc", { .minTime = ids[0], .count = 2 });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).first, ids[0]);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).first, ids[1]);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);
}

TEST(RedisAdapter, StatusListener)
{
  RedisAdapter redis("TEST");

  //  this status should not be seen
  EXPECT_TRUE(redis.setStatus("FAIL"));

  //  add status reader
  bool waiting = true;
  EXPECT_TRUE(redis.addStatusReader([&](const string& base, const string& sub, const RA::TimeValList<string>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_EQ(sub.size(), 0);
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first, 0);
      EXPECT_STREQ(ats[0].second.c_str(), "OK");
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger status reader
  EXPECT_TRUE(redis.setStatus("OK"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  // should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeStatusReader());
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  waiting = true;
  EXPECT_TRUE(redis.setStatus("FAIL"));

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
  EXPECT_TRUE(redis.addLogReader([&](const string& base, const string& sub, const RA::TimeValList<string>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_EQ(sub.size(), 0);
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first, 0);
      EXPECT_STREQ(ats[0].second.c_str(), "log 2");
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

TEST(RedisAdapter, DataListener)
{
  RedisAdapter redis("TEST");

  vector<float> vf = { 1, 2, 3 };

  //  this should not be seen
  EXPECT_GT(redis.addStreamSingleList("xyz", vf), 0);

  //  add reader
  bool waiting = true;
  EXPECT_TRUE(redis.addStreamListReader<float>("xyz", [&](const string& base, const string& sub, const RA::TimeValList<vector<float>>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_GT(ats.size(), 0);
      EXPECT_GT(ats[0].first, 0);
      EXPECT_EQ(ats[0].second.size(), 3);
      EXPECT_FLOAT_EQ(ats[0].second[0], 1.23);
      EXPECT_FLOAT_EQ(ats[0].second[1], 3.45);
      EXPECT_FLOAT_EQ(ats[0].second[2], 5.67);
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger reader
  vf[0] = 1.23; vf[1] = 3.45; vf[2] = 5.67;
  EXPECT_GT(redis.addStreamSingleList("xyz", vf), 0);

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeStreamReader("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  vf[0] = 0; vf[1] = 0; vf[2] = 0;
  waiting = true;
  EXPECT_GT(redis.addStreamSingleList("xyz", vf), 0);

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

  EXPECT_TRUE(redis.deleteStream("dstdat"));

  int val = 1;
  EXPECT_GT(redis.addStreamSingle("srcdat", val), 0);
  EXPECT_TRUE(redis.copyStream("srcdat", "dstdat"));
  EXPECT_GT(redis.getStreamSingle("dstdat", val), 0);
  EXPECT_EQ(val, 1);

  EXPECT_TRUE(redis.deleteStream("dstdat"));

  val = 1;
  EXPECT_GT(redis.addStreamSingle("srcdat", val), 0);
  EXPECT_TRUE(redis.renameStream("srcdat", "dstdat"));
  EXPECT_GT(redis.getStreamSingle("dstdat", val), 0);
  EXPECT_EQ(val, 1);

  uint64_t nanos = redis.getServerTime();
  EXPECT_GT(nanos, 0);
}
