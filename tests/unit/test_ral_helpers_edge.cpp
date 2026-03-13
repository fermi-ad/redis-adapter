//
//  test_ral_helpers_edge.cpp
//
//  Edge case tests for RAL_Helpers serialization functions
//

#include <gtest/gtest.h>
#include "RAL_Helpers.hpp"
#include <cmath>
#include <limits>

TEST(RAL_HelpersEdge, DoubleInfinity)
{
  double inf = std::numeric_limits<double>::infinity();
  auto attrs = ral_from_double(inf);
  auto val = ral_to_double(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_TRUE(std::isinf(*val));
  EXPECT_GT(*val, 0);
}

TEST(RAL_HelpersEdge, DoubleNegInfinity)
{
  double ninf = -std::numeric_limits<double>::infinity();
  auto attrs = ral_from_double(ninf);
  auto val = ral_to_double(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_TRUE(std::isinf(*val));
  EXPECT_LT(*val, 0);
}

TEST(RAL_HelpersEdge, DoubleNaN)
{
  double nan = std::numeric_limits<double>::quiet_NaN();
  auto attrs = ral_from_double(nan);
  auto val = ral_to_double(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_TRUE(std::isnan(*val));
}

TEST(RAL_HelpersEdge, DoubleSmallestSubnormal)
{
  double denorm = std::numeric_limits<double>::denorm_min();
  auto attrs = ral_from_double(denorm);
  auto val = ral_to_double(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, denorm);
}

TEST(RAL_HelpersEdge, IntMinMax)
{
  auto attrs_min = ral_from_int(INT64_MIN);
  auto val_min = ral_to_int(attrs_min);
  ASSERT_TRUE(val_min.has_value());
  EXPECT_EQ(*val_min, INT64_MIN);

  auto attrs_max = ral_from_int(INT64_MAX);
  auto val_max = ral_to_int(attrs_max);
  ASSERT_TRUE(val_max.has_value());
  EXPECT_EQ(*val_max, INT64_MAX);
}

TEST(RAL_HelpersEdge, LargeBlob)
{
  // 1MB blob
  std::vector<uint8_t> blob(1024 * 1024, 0xAB);
  auto attrs = ral_from_blob(blob.data(), blob.size());
  auto val = ral_to_blob(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(val->size(), blob.size());
  EXPECT_EQ((*val)[0], 0xAB);
  EXPECT_EQ((*val).back(), 0xAB);
}

TEST(RAL_HelpersEdge, BlobWithNullBytes)
{
  std::vector<uint8_t> blob = {0x00, 0x01, 0x00, 0xFF, 0x00};
  auto attrs = ral_from_blob(blob.data(), blob.size());
  auto val = ral_to_blob(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, blob);
}

TEST(RAL_HelpersEdge, StringWithBinaryContent)
{
  std::string data = "hello\x00world";
  data.push_back('\0');
  data += "end";
  auto attrs = ral_from_string(data);
  auto val = ral_to_string(attrs);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, data);
}

TEST(RAL_HelpersEdge, WrongFieldName)
{
  Attrs attrs;
  attrs["wrong_field"] = "some_value";
  auto str = ral_to_string(attrs);
  EXPECT_FALSE(str.has_value());
  auto dbl = ral_to_double(attrs);
  EXPECT_FALSE(dbl.has_value());
  auto integer = ral_to_int(attrs);
  EXPECT_FALSE(integer.has_value());
}

TEST(RAL_HelpersEdge, DoubleWrongSize)
{
  Attrs attrs;
  attrs["_"] = "abc";  // wrong size for double
  auto val = ral_to_double(attrs);
  EXPECT_FALSE(val.has_value());
}

TEST(RAL_HelpersEdge, IntWrongSize)
{
  Attrs attrs;
  attrs["_"] = "abc";  // wrong size for int64_t
  auto val = ral_to_int(attrs);
  EXPECT_FALSE(val.has_value());
}
