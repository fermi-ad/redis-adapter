//
//  test_ral_time_edge.cpp
//
//  Edge case and boundary tests for RAL_Time
//

#include <gtest/gtest.h>
#include "RAL_Time.hpp"
#include <string>
#include <climits>

TEST(RAL_TimeEdge, MultipleDashes)
{
  // Parser splits on first dash: ms="1", sub="2-3" -> stoul("2-3") gets 2
  RAL_Time t("1-2-3");
  EXPECT_EQ(t.value, 1'000'000 + 2);
}

TEST(RAL_TimeEdge, TrailingDash)
{
  RAL_Time t("100-");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeEdge, MaxInt64)
{
  int64_t max_val = INT64_MAX;
  RAL_Time t(max_val);
  EXPECT_TRUE(t.ok());
  std::string id = t.id();
  RAL_Time parsed(id);
  EXPECT_EQ(parsed.value, max_val);
}

TEST(RAL_TimeEdge, NegativeValue)
{
  RAL_Time t(-1);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.err(), 1u);
}

TEST(RAL_TimeEdge, IdOrMinMax)
{
  RAL_Time bad(-1);
  EXPECT_EQ(bad.id_or_min(), "-");
  EXPECT_EQ(bad.id_or_max(), "+");
}

TEST(RAL_TimeEdge, ValidTimeIdOrNow)
{
  RAL_Time t(5'000'000LL);  // 5ms-0
  EXPECT_EQ(t.id_or_now(), "5-0");
}

TEST(RAL_TimeEdge, WhitespaceInput)
{
  RAL_Time t("  ");
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(t.value, 0);
}

TEST(RAL_TimeEdge, ZeroZero)
{
  RAL_Time t("0-0");
  EXPECT_EQ(t.value, 0);
  EXPECT_FALSE(t.ok());
}

TEST(RAL_TimeEdge, LargeSubId)
{
  RAL_Time t("1-999999");
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(t.value, 1'000'000 + 999'999);
}

TEST(RAL_TimeEdge, ConversionOperators)
{
  RAL_Time t(42'000'000LL);
  int64_t  i = t;
  uint64_t u = t;
  EXPECT_EQ(i, 42'000'000LL);
  EXPECT_EQ(u, 42'000'000ULL);
}
