//
//  test_connection.cpp
//
//  Integration tests for Redis connection
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

TEST(Connection, Connected)
{
  RedisAdapterLite redis("TEST");
  EXPECT_TRUE(redis.connected());
}

TEST(Connection, ExitNotConnected)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) exit(1);
}
