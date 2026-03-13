//
//  test_utility.cpp
//
//  Integration tests for del, copy, rename
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

TEST(Utility, Del)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addInt("t_del", 42).ok());

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_del", val).ok());
  EXPECT_EQ(val, 42);

  EXPECT_TRUE(redis.del("t_del"));
  EXPECT_FALSE(redis.getInt("t_del", val).ok());
}

TEST(Utility, DelNonexistent)
{
  RedisAdapterLite redis("TEST");
  EXPECT_TRUE(redis.del("t_del_noexist"));
}

TEST(Utility, Copy)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_copy_src");
  redis.del("t_copy_dst");

  EXPECT_TRUE(redis.addInt("t_copy_src", 77).ok());
  EXPECT_TRUE(redis.copy("t_copy_src", "t_copy_dst"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_copy_dst", val).ok());
  EXPECT_EQ(val, 77);
}

TEST(Utility, Rename)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ren_src");
  redis.del("t_ren_dst");

  EXPECT_TRUE(redis.addInt("t_ren_src", 88).ok());
  EXPECT_TRUE(redis.rename("t_ren_src", "t_ren_dst"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_ren_dst", val).ok());
  EXPECT_EQ(val, 88);

  EXPECT_FALSE(redis.getInt("t_ren_src", val).ok());
}

TEST(Utility, CopyWithBaseKey)
{
  RedisAdapterLite redis("TEST");
  RedisAdapterLite redis2("OTHER");
  redis.del("t_cpbk_dst");

  EXPECT_TRUE(redis2.addInt("t_cpbk_src", 55).ok());
  EXPECT_TRUE(redis.copy("t_cpbk_src", "t_cpbk_dst", "OTHER"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_cpbk_dst", val).ok());
  EXPECT_EQ(val, 55);
}
