//
//  test_timestamps.cpp
//
//  Integration tests for stream timestamp behavior
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

TEST(Timestamps, Increasing)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ts_inc");

  RAL_Time t1 = redis.addInt("t_ts_inc", 1, { .trim = 0 });
  RAL_Time t2 = redis.addInt("t_ts_inc", 2, { .trim = 0 });
  RAL_Time t3 = redis.addInt("t_ts_inc", 3, { .trim = 0 });

  EXPECT_TRUE(t1.ok());
  EXPECT_TRUE(t2.ok());
  EXPECT_TRUE(t3.ok());
  EXPECT_LT(t1.value, t2.value);
  EXPECT_LT(t2.value, t3.value);
}

TEST(Timestamps, GetAtSpecificTime)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ts_at");

  RAL_Time id1 = redis.addInt("t_ts_at", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_ts_at", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_ts_at", 30, { .trim = 0 });

  int64_t val = 0;
  RAL_Time got = redis.getInt("t_ts_at", val, { .maxTime = id2 });
  EXPECT_TRUE(got.ok());
  EXPECT_EQ(val, 20);
  EXPECT_EQ(got.value, id2.value);
}
