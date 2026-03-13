//
//  test_time_parse.cpp
//
//  Regression: RAL_Time silently swallowed parse exceptions
//
//  Bug: The RAL_Time(const string&) constructor used a bare catch(...)
//  that silently converted all parse failures to value=0 with no
//  logging. This made it impossible to debug malformed stream IDs.
//
//  Fix: Parse failures now log the failed input and exception message
//  via syslog before setting value=0.
//

#include <gtest/gtest.h>
#include "RAL_Time.hpp"

TEST(TimeParse, EmptyString)
{
  RAL_Time t("");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeParse, GarbageInput)
{
  RAL_Time t("abc-def");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeParse, DashOnly)
{
  RAL_Time t("-");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeParse, OverflowInput)
{
  RAL_Time t("999999999999999999999-0");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeParse, ValidZero)
{
  RAL_Time t("0-0");
  EXPECT_EQ(t.value, 0);
  EXPECT_FALSE(t.ok());
}

TEST(TimeParse, ValidSmall)
{
  RAL_Time t("1-0");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 1'000'000);
}

TEST(TimeParse, ValidWithSubId)
{
  RAL_Time t("100-5");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 100'000'000 + 5);
}

TEST(TimeParse, PartialGarbage)
{
  RAL_Time t("100-xyz");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(TimeParse, NoDash)
{
  RAL_Time t("500");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 500'000'000);
}

TEST(TimeParse, RoundTripPreservation)
{
  RAL_Time orig(123456789012345LL);
  std::string id = orig.id();
  RAL_Time parsed(id);
  EXPECT_EQ(parsed.value, orig.value);
}
