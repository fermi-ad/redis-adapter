//
//  test_boundary.cpp
//
//  Boundary condition tests: count limits, time ranges, trim behavior
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <thread>
#include <chrono>

using namespace std;

TEST(Boundary, CountExceedsAvailable)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_count");
  redis.addInt("t_bound_count", 1);
  redis.addInt("t_bound_count", 2);
  redis.addInt("t_bound_count", 3);

  auto results = redis.getInts("t_bound_count", {.count = 100});
  EXPECT_EQ(results.size(), 3u);
  redis.del("t_bound_count");
}

TEST(Boundary, CountOne)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_one");
  redis.addInt("t_bound_one", 10);
  redis.addInt("t_bound_one", 20);
  redis.addInt("t_bound_one", 30);

  auto results = redis.getInts("t_bound_one", {.count = 1});
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].second, 10);  // Forward: oldest first
  redis.del("t_bound_one");
}

TEST(Boundary, BeforeCountOne)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_bef1");
  redis.addInt("t_bound_bef1", 10);
  redis.addInt("t_bound_bef1", 20);
  redis.addInt("t_bound_bef1", 30);

  auto results = redis.getIntsBefore("t_bound_bef1", {.count = 1});
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].second, 30);  // Reverse: most recent
  redis.del("t_bound_bef1");
}

TEST(Boundary, TrimToZeroStillAdds)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_trim0");

  auto t = redis.addInt("t_bound_trim0", 42, {.trim = 0});
  EXPECT_TRUE(t.ok());

  int64_t val = 0;
  redis.getInt("t_bound_trim0", val);
  EXPECT_EQ(val, 42);
  redis.del("t_bound_trim0");
}

TEST(Boundary, BulkAddSingleItem)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_bulk1");

  TimeIntList data = {{RAL_Time(), 42}};
  auto ids = redis.addInts("t_bound_bulk1", data);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_TRUE(ids[0].ok());

  int64_t val = 0;
  redis.getInt("t_bound_bulk1", val);
  EXPECT_EQ(val, 42);
  redis.del("t_bound_bulk1");
}

TEST(Boundary, MinTimeAfterMaxTime)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_minmax");
  redis.addInt("t_bound_minmax", 42);

  auto results = redis.getInts("t_bound_minmax", {
    .minTime = RAL_Time(99999999999000000LL),
    .maxTime = RAL_Time(1000000LL)
  });
  EXPECT_TRUE(results.empty());
  redis.del("t_bound_minmax");
}

TEST(Boundary, GetFromEmptyStream)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_empty_get");

  int64_t val = -1;
  auto t = redis.getInt("t_bound_empty_get", val);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(val, -1);  // unchanged
}

TEST(Boundary, LargeBlob)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_bigblob");

  // 512KB blob
  vector<uint8_t> blob(512 * 1024, 0xCD);
  auto t = redis.addBlob("t_bound_bigblob", blob.data(), blob.size());
  EXPECT_TRUE(t.ok());

  vector<uint8_t> dest;
  auto t2 = redis.getBlob("t_bound_bigblob", dest);
  EXPECT_TRUE(t2.ok());
  EXPECT_EQ(dest.size(), blob.size());
  EXPECT_EQ(dest, blob);
  redis.del("t_bound_bigblob");
}

TEST(Boundary, ManyFields)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_fields");

  Attrs attrs;
  for (int i = 0; i < 50; i++)
    attrs["field_" + to_string(i)] = "value_" + to_string(i);

  auto t = redis.addAttrs("t_bound_fields", attrs);
  EXPECT_TRUE(t.ok());

  Attrs dest;
  auto t2 = redis.getAttrs("t_bound_fields", dest);
  EXPECT_TRUE(t2.ok());
  EXPECT_EQ(dest.size(), 50u);
  EXPECT_EQ(dest["field_0"], "value_0");
  EXPECT_EQ(dest["field_49"], "value_49");
  redis.del("t_bound_fields");
}

TEST(Boundary, RapidAddGetCycle)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bound_rapid");

  for (int i = 0; i < 100; i++)
  {
    redis.addInt("t_bound_rapid", i);
    int64_t val = -1;
    auto t = redis.getInt("t_bound_rapid", val);
    EXPECT_TRUE(t.ok());
    EXPECT_EQ(val, i);
  }
  redis.del("t_bound_rapid");
}
