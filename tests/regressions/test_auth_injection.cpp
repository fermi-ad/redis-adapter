//
//  test_auth_injection.cpp
//
//  Regression: AUTH command used format strings
//
//  Bug: redisCommand(ctx, "AUTH %s %s", user, password) used printf-style
//  format strings. Passwords containing %, spaces, or quotes could produce
//  malformed Redis commands or undefined behavior.
//
//  Fix: Replaced with redisCommandArgv using binary-safe argv arrays.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

TEST(AuthInjection, ConnectionWithoutAuth)
{
  RAL_Options opts;
  RedisAdapterLite redis("TEST_AUTH_FIX", opts);
  EXPECT_TRUE(redis.connected());
}

TEST(AuthInjection, ConnectionWithBadAuth)
{
  RAL_Options opts;
  opts.user = "nonexistent user with spaces";
  opts.password = "bad password with \"quotes\" and 'specials'";
  RedisAdapterLite redis("TEST_AUTH_BAD", opts);
  // Should not crash or produce malformed commands
}

TEST(AuthInjection, PasswordWithPercentSign)
{
  RAL_Options opts;
  opts.user = "default";
  opts.password = "pass%sword%d%s%%";
  RedisAdapterLite redis("TEST_AUTH_PCT", opts);
  // Previously, %s and %d in the password would be interpreted as
  // format specifiers, causing undefined behavior or crashes
}
