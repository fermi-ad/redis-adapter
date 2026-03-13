//
//  test_time_overflow.cpp
//
//  Bug: RAL_Time(string) could overflow int64_t when parsing stream IDs
//  with very large millisecond timestamps. The multiplication
//  `ms * NANOS_PER_MILLI` would silently wrap, producing garbage values.
//
//  Fix: Added overflow guard that checks ms > INT64_MAX / NANOS_PER_MILLI
//  before multiplying, returning value=0 on overflow.
//

#include <gtest/gtest.h>
#include "RAL_Time.hpp"
#include <string>
#include <climits>

static const uint32_t NANOS_PER_MILLI = 1'000'000;

TEST(TimeOverflow, NormalTimestampNoOverflow)
{
  // Current epoch ~1.7 trillion ms — well within range
  RAL_Time t("1700000000000-0");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 1700000000000LL * NANOS_PER_MILLI);
}

TEST(TimeOverflow, MaxSafeTimestamp)
{
  // Largest ms value that fits: INT64_MAX / NANOS_PER_MILLI
  int64_t max_ms = INT64_MAX / NANOS_PER_MILLI;
  std::string id = std::to_string(max_ms) + "-0";
  RAL_Time t(id);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, max_ms * NANOS_PER_MILLI);
}

TEST(TimeOverflow, OverflowByOne)
{
  // One past the max — should detect overflow and return 0
  int64_t overflow_ms = INT64_MAX / NANOS_PER_MILLI + 1;
  std::string id = std::to_string(overflow_ms) + "-0";
  RAL_Time t(id);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeOverflow, MassiveOverflow)
{
  // Way past max — should detect overflow
  RAL_Time t("9999999999999999-0");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeOverflow, SubIdAdditionNoOverflow)
{
  // Verify sub-ID is added correctly for safe timestamps
  RAL_Time t("1000-42");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 1000LL * NANOS_PER_MILLI + 42);
}

TEST(TimeOverflow, RoundTripAtMaxSafe)
{
  int64_t max_ms = INT64_MAX / NANOS_PER_MILLI;
  std::string id = std::to_string(max_ms) + "-0";
  RAL_Time t(id);
  EXPECT_TRUE(t.ok());

  // Round-trip through id() should preserve the value
  RAL_Time t2(t.id());
  EXPECT_EQ(t.value, t2.value);
}
