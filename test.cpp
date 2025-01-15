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

TEST(RedisAdapter, UnixDomainSocket)
{
    //If trying to connect with a domain socket fails, for some unknown reason, this can cause a meemory corruption
    //when the redis object is deleted. This causes the rest of the tests to fails.
    //To work around this we should use a pointer to the RedisAdapter object and delete it explicitly.
    //Assumes the socket file is in the /tmp directory and test is run from the build directory

    RA_Options opts; opts.cxn.path = "/tmp/redis.sock";
    auto redis = make_unique<RedisAdapter>("TEST", opts);
    RA_Options opts; opts.cxn.path = "/tmp/redis.sock";
    auto redis = make_unique<RedisAdapter>("TEST", opts);

    EXPECT_TRUE(redis->connected()) << "Failed to connect to the Redis server using Unix domain socket.";
}

TEST(RedisAdapter, DataSingle)
{
  RedisAdapter redis("TEST");

  //  set/get string single element
  EXPECT_TRUE(redis.addSingleValue("abc", "xxx").ok());
  string s;
  EXPECT_TRUE(redis.getSingleValue("abc", s).ok());
  EXPECT_STREQ(s.c_str(), "xxx");

  //  set/get float single element
  EXPECT_TRUE(redis.addSingleValue("abc", 1.23f).ok());  //  Note value MUST have 'f' suffix else it's a double
  float f = 0;
  EXPECT_TRUE(redis.getSingleValue("abc", f).ok());
  EXPECT_FLOAT_EQ(f, 1.23f);

  //  set/get float single element
  EXPECT_TRUE(redis.addSingleValue<float>("abc", 1.23).ok());  //  Here it's OK we are calling specialization <float>
  EXPECT_TRUE(redis.getSingleValue("abc", f).ok());
  EXPECT_FLOAT_EQ(f, 1.23f);

  //  set/get double single element
  EXPECT_TRUE(redis.addSingleDouble("abc", 1.23).ok());
  double d = 0;
  EXPECT_TRUE(redis.getSingleValue("abc", d).ok());
  EXPECT_DOUBLE_EQ(d, 1.23);

  //  set/get float vector single element
  vector<float> vf = { 1.23, 3.45, 5.67 };
  EXPECT_TRUE(redis.addSingleList("abc", vf).ok());
  vf.clear();
  EXPECT_TRUE(redis.getSingleList("abc", vf).ok());
  EXPECT_EQ(vf.size(), 3);
  EXPECT_FLOAT_EQ(vf[0], 1.23);
  EXPECT_FLOAT_EQ(vf[1], 3.45);
  EXPECT_FLOAT_EQ(vf[2], 5.67);

  //  set/get int array single element (also span if c++20)
  array<int, 3> ai = { 1, 2, 3 };
  EXPECT_TRUE(redis.addSingleList("abc", ai).ok());
  //  note it comes back as a vector
  vector<int> vi;
  EXPECT_TRUE(redis.getSingleList("abc", vi).ok());
  EXPECT_EQ(vi.size(), 3);
  EXPECT_EQ(vi[0], 1);
  EXPECT_EQ(vi[1], 2);
  EXPECT_EQ(vi[2], 3);
}

TEST(RedisAdapter, Data)
{
  RedisAdapter redis("TEST");

  //  set/get data
  RA_Time idA = redis.addSingleValue("abc", "xxx");
  EXPECT_TRUE(idA.ok());
  RA_Time idB = redis.addSingleValue("abc", "yyy");
  EXPECT_TRUE(idB.ok());
  auto is_str = redis.getValues<string>("abc", { .minTime=idA, .maxTime=idB });
  EXPECT_EQ(is_str.size(), 2);
  EXPECT_TRUE(is_str.at(0).first.ok());
  EXPECT_STREQ(is_str.at(0).second.c_str(), "xxx");
  EXPECT_TRUE(is_str.at(1).first.ok());
  EXPECT_STREQ(is_str.at(1).second.c_str(), "yyy");

  //  add multiple data
  RA::TimeValList<int> is_int = {{ 0, 1 }, { 0, 2 }, { 0, 3 }};
  auto ids = redis.addValues("abc", is_int);
  EXPECT_EQ(ids.size(), 3);
  EXPECT_TRUE(ids[0].ok());
  EXPECT_TRUE(ids[1].ok());
  EXPECT_TRUE(ids[2].ok());

  //  get data after
  is_int = redis.getValuesAfter<int>("abc", { .minTime = ids[0], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_TRUE(is_int.at(0).first.ok());
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_TRUE(is_int.at(1).first.ok());
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_TRUE(is_int.at(2).first.ok());
  EXPECT_EQ(is_int.at(2).second, 3);

  //  get data before
  is_int = redis.getValuesBefore<int>("abc", { .maxTime = ids[2], .count = 3 });
  EXPECT_EQ(is_int.size(), 3);
  EXPECT_TRUE(is_int.at(0).first.ok());
  EXPECT_EQ(is_int.at(0).second, 1);
  EXPECT_TRUE(is_int.at(1).first.ok());
  EXPECT_EQ(is_int.at(1).second, 2);
  EXPECT_TRUE(is_int.at(2).first.ok());
  EXPECT_EQ(is_int.at(2).second, 3);

  //  add/get Attrs data
  RA::TimeValList<RA::Attrs> is_at = {{ 0, {{ "a", "1" }, { "b", "2" }}}};
  ids = redis.addValues("abc", is_at);
  EXPECT_EQ(ids.size(), 1);
  EXPECT_TRUE(ids[0].ok());
  is_at = redis.getValues<RA::Attrs>("abc", { .minTime=ids[0], .maxTime=ids[0] });
  EXPECT_EQ(is_at.size(), 1);
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
  auto ids = redis.addLists("abc", is_vf);
  EXPECT_EQ(ids.size(), 2);
  EXPECT_TRUE(ids[0].ok());
  EXPECT_TRUE(ids[1].ok());

  //  get float vector data
  is_vf = redis.getLists<float>("abc", { .minTime=ids[0], .maxTime=ids[1] });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data before
  is_vf = redis.getListsBefore<float>("abc", { .maxTime = ids[1], .count = 2 });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);

  //  get float vector data after
  is_vf = redis.getListsAfter<float>("abc", { .minTime = ids[0], .count = 2 });
  EXPECT_EQ(is_vf.size(), 2);
  EXPECT_EQ(is_vf.at(0).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[0], 1.1);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[1], 1.2);
  EXPECT_FLOAT_EQ(is_vf.at(0).second[2], 1.3);
  EXPECT_EQ(is_vf.at(1).second.size(), 3);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[0], 2.1);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[1], 2.2);
  EXPECT_FLOAT_EQ(is_vf.at(1).second[2], 2.3);
}

TEST(RedisAdapter, DataReader)
{
  RedisAdapter redis("TEST");

  vector<float> vf = { 1, 2, 3 };

  //  this should not be seen
  EXPECT_TRUE(redis.addSingleList("xyz", vf).ok());

  //  add reader
  bool waiting = true;
  EXPECT_TRUE(redis.addListsReader<float>("xyz", [&](const string& base, const string& sub, const RA::TimeValList<vector<float>>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "xyz");
      EXPECT_GT(ats.size(), 0);
      EXPECT_TRUE(ats[0].first.ok());
      EXPECT_EQ(ats[0].second.size(), 3);
      EXPECT_FLOAT_EQ(ats[0].second[0], 1.23);
      EXPECT_FLOAT_EQ(ats[0].second[1], 3.45);
      EXPECT_FLOAT_EQ(ats[0].second[2], 5.67);
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  trigger reader
  vf[0] = 1.23; vf[1] = 3.45; vf[2] = 5.67;
  EXPECT_TRUE(redis.addSingleList("xyz", vf).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  remove status reader
  EXPECT_TRUE(redis.removeReader("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger reader
  vf[0] = 0; vf[1] = 0; vf[2] = 0;
  waiting = true;
  EXPECT_TRUE(redis.addSingleList("xyz", vf).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, DeferReader)
{
  RedisAdapter redis("TEST");

  bool waiting = false;
  EXPECT_TRUE(redis.setDeferReaders(true));

  //  add readers
  EXPECT_TRUE(redis.addValuesReader<int>("rrr", [&](const string& base, const string& sub, const RA::TimeValList<int>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "rrr");
      EXPECT_GT(ats.size(), 0);
      EXPECT_TRUE(ats[0].first.ok());
      EXPECT_EQ(ats[0].second, 3);
    }
  ));
  EXPECT_TRUE(redis.addValuesReader<int>("sss", [&](const string& base, const string& sub, const RA::TimeValList<int>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "sss");
      EXPECT_GT(ats.size(), 0);
      EXPECT_TRUE(ats[0].first.ok());
      EXPECT_EQ(ats[0].second, 4);
    }
  ));
  EXPECT_TRUE(redis.addValuesReader<int>("ttt", [&](const string& base, const string& sub, const RA::TimeValList<int>& ats)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.c_str(), "ttt");
      EXPECT_GT(ats.size(), 0);
      EXPECT_TRUE(ats[0].first.ok());
      EXPECT_EQ(ats[0].second, 5);
    }
  ));
  this_thread::sleep_for(milliseconds(5));

  //  this should not be seen
  waiting = true;
  EXPECT_TRUE(redis.addSingleValue("sss", 1).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);

  EXPECT_TRUE(redis.setDeferReaders(false));
  this_thread::sleep_for(milliseconds(5));

  //  trigger reader
  waiting = true;
  EXPECT_TRUE(redis.addSingleValue("sss", 4).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  trigger reader
  waiting = true;
  EXPECT_TRUE(redis.addSingleValue("rrr", 3).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  trigger reader
  waiting = true;
  EXPECT_TRUE(redis.addSingleValue("ttt", 5).ok());

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  EXPECT_TRUE(redis.setDeferReaders(true));

  //  remove readers
  EXPECT_TRUE(redis.removeReader("rrr"));
  EXPECT_TRUE(redis.removeReader("sss"));
  EXPECT_TRUE(redis.removeReader("ttt"));

  EXPECT_TRUE(redis.setDeferReaders(false));
  this_thread::sleep_for(milliseconds(5));

  //  this should not be seen
  waiting = true;
  EXPECT_TRUE(redis.addSingleValue("sss", 1).ok());

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
  EXPECT_TRUE(redis.publish("zzz", "001"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);

  //  unsubscribe
  EXPECT_TRUE(redis.unsubscribe("xyz"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("xyz", "002"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, PSubscribe)
{
  RedisAdapter redis("TEST");

  //  this publish should not be seen
  EXPECT_TRUE(redis.publish("xyz", "000"));

  //  subscribe
  bool waiting = true;
  EXPECT_TRUE(redis.psubscribe("xyz*", [&](const string& base, const string& sub, const string& msg)
    {
      waiting = false;
      EXPECT_STREQ(base.c_str(), "TEST");
      EXPECT_STREQ(sub.substr(0, 3).c_str(), "xyz");
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

  //  trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("xyz:abc", "123"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should not be waiting anymore
  EXPECT_FALSE(waiting);

  //  dont trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("zzz", "001"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  should still be waiting
  EXPECT_TRUE(waiting);

  //  unsubscribe
  EXPECT_TRUE(redis.unsubscribe("xyz*"));
  this_thread::sleep_for(milliseconds(5));

  //  try to trigger subscription
  waiting = true;
  EXPECT_TRUE(redis.publish("xyz", "002"));

  for (int i = 0; i < 20 && waiting; i++)
    this_thread::sleep_for(milliseconds(5));

  //  no wildcards allowed in base key
  EXPECT_FALSE(redis.psubscribe("xyz*", {}, "fgh*"));

  //  should still be waiting
  EXPECT_TRUE(waiting);
}

TEST(RedisAdapter, Utility)
{
  RedisAdapter redis("TEST");

  EXPECT_TRUE(redis.del("dstdat"));

  int val = 1;
  EXPECT_TRUE(redis.addSingleValue("srcdat", val).ok());
  EXPECT_TRUE(redis.copy("srcdat", "dstdat"));
  EXPECT_TRUE(redis.getSingleValue("dstdat", val).ok());
  EXPECT_EQ(val, 1);

  EXPECT_TRUE(redis.del("dstdat"));

  val = 1;
  EXPECT_TRUE(redis.addSingleValue("srcdat", val).ok());
  EXPECT_TRUE(redis.rename("srcdat", "dstdat"));
  EXPECT_TRUE(redis.getSingleValue("dstdat", val).ok());
  EXPECT_EQ(val, 1);
}

TEST(RedisAdapter, Watchdog)
{
  RedisAdapter redis("TEST", { .dogname = "TEST" });

  //  wait a bit and check auto-watchdog is there
  this_thread::sleep_for(milliseconds(100));
  EXPECT_EQ(redis.getWatchdogs().size(), 1);

  //  add manual watchdog
  EXPECT_TRUE(redis.addWatchdog("SPOT", 1));

  //  wait a bit and check both are there
  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2);

  //  pet manual dog, wait past initial expire time, and check again
  EXPECT_TRUE(redis.petWatchdog("SPOT", 1));
  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2);

  //  wait past expire time and check manual dog gone
  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 1);
}
