//
//  test_ral_time.cpp
//
//  Unit tests for RAL_Time nanosecond-precision timestamp
//

#include <gtest/gtest.h>
#include "RAL_Time.hpp"

TEST(RAL_Time, DefaultZero)
{
  RAL_Time t;
  EXPECT_EQ(t.value, 0);
  EXPECT_FALSE(t.ok());
}

TEST(RAL_Time, PositiveOk)
{
  RAL_Time t(1000000);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(static_cast<int64_t>(t), 1000000);
  EXPECT_EQ(static_cast<uint64_t>(t), 1000000u);
}

TEST(RAL_Time, NegativeNotOk)
{
  RAL_Time t(-5);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(static_cast<int64_t>(t), 0);
  EXPECT_EQ(t.err(), 5u);
}

TEST(RAL_Time, NotConnectedError)
{
  EXPECT_FALSE(RAL_NOT_CONNECTED.ok());
  EXPECT_EQ(RAL_NOT_CONNECTED.err(), 1u);
}

TEST(RAL_Time, IdRoundTrip)
{
  RAL_Time orig(123456789012345LL);
  std::string id = orig.id();
  RAL_Time parsed(id);
  EXPECT_EQ(parsed.value, orig.value);
}

TEST(RAL_Time, IdOrMinMax)
{
  RAL_Time valid(1000000);
  EXPECT_NE(valid.id_or_min(), "-");
  EXPECT_NE(valid.id_or_max(), "+");

  RAL_Time invalid;
  EXPECT_EQ(invalid.id_or_min(), "-");
  EXPECT_EQ(invalid.id_or_max(), "+");
}

TEST(RAL_Time, ParseBadString)
{
  RAL_Time t("not-a-number");
  EXPECT_FALSE(t.ok());
}
