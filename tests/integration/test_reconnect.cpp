//
//  test_reconnect.cpp
//
//  Tests for reconnection behavior and connected() status
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

using namespace std;

TEST(Reconnect, ConnectedAfterConstruction)
{
  RedisAdapterLite redis("TEST");
  EXPECT_TRUE(redis.connected());
}

TEST(Reconnect, BadHostNotConnected)
{
  RAL_Options opts;
  opts.host = "192.0.2.1";  // RFC 5737 TEST-NET, unreachable
  opts.timeout = 100;       // fast timeout
  RedisAdapterLite redis("TEST_BAD", opts);
  EXPECT_FALSE(redis.connected());
}

TEST(Reconnect, BadPortNotConnected)
{
  RAL_Options opts;
  opts.port = 1;     // unlikely to have Redis on port 1
  opts.timeout = 100;
  RedisAdapterLite redis("TEST_BADPORT", opts);
  EXPECT_FALSE(redis.connected());
}

TEST(Reconnect, OperationsAfterBadConnection)
{
  RAL_Options opts;
  opts.host = "192.0.2.1";
  opts.timeout = 100;
  RedisAdapterLite redis("TEST_BAD_OPS", opts);

  auto t = redis.addInt("key", 42);
  EXPECT_FALSE(t.ok());

  int64_t val = -1;
  auto t2 = redis.getInt("key", val);
  EXPECT_FALSE(t2.ok());
  EXPECT_EQ(val, -1);
}
