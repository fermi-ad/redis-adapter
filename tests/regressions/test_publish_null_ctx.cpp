//
//  test_publish_null_ctx.cpp
//
//  Bug: HiredisConnection::publish() did not check for null _ctx
//  before calling redisCommandArgv, unlike every other method.
//  If the connection was down, this would crash (UB from null ptr).
//
//  Fix: Added `if (!_ctx) return -1;` guard at the top of publish().
//

#include <gtest/gtest.h>
#include "HiredisConnection.hpp"

// Connect to an invalid port so _ctx is null after connection failure
TEST(PublishNullCtx, PublishOnDeadConnectionReturnsError)
{
  RAL_Options opts;
  opts.host = "127.0.0.1";
  opts.port = 1;      // nothing listens here
  opts.timeout = 50;

  HiredisConnection conn(opts);

  // This should return -1 without crashing (was UB before fix)
  int64_t result = conn.publish("test-channel", "hello");
  EXPECT_EQ(result, -1);
}

// Verify that publish still works on a live connection
TEST(PublishNullCtx, PublishOnLiveConnectionWorks)
{
  RAL_Options opts;
  opts.timeout = 500;

  HiredisConnection conn(opts);
  if (!conn.ping()) GTEST_SKIP() << "Redis not available";

  int64_t result = conn.publish("t_null_ctx_live", "test");
  EXPECT_GE(result, 0);
}
